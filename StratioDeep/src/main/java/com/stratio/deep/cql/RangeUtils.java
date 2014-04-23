/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.cql;

import com.datastax.driver.core.*;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.utils.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.*;

import static com.google.common.collect.Iterables.*;

/**
 * {@link CqlPagingRecordReader} implementation that returns an instance of a
 * {@link DeepRecordReader}.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class RangeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RangeUtils.class);

    private RangeUtils() {
    }

    /**
     * Gets the list of token for each cluster machine.<br/>
     * The concrete class of the token depends on the partitioner used.<br/>
     *
     * @param query           the query to execute against the given session to obtain the list of tokens.
     * @param sessionWithHost the pair object containing both the session and the name of the machine to which we're connected to.
     * @param partitioner     the partitioner used in the cluster.
     * @return a map containing, for each cluster machine, the list of tokens. Tokens are not returned in any particular order.
     */
    static Map<String, Iterable<Comparable>> fetchTokens(
            String query, final Pair<Session, String> sessionWithHost, IPartitioner partitioner) {

        ResultSet rSet = sessionWithHost.left.execute(query);

        final AbstractType tkValidator = partitioner.getTokenValidator();
        final Map<String, Iterable<Comparable>> tokens = Maps.newHashMap();

        Iterable<Pair<String, Iterable<Comparable>>> pairs =
                transform(rSet.all(), new Function<Row, Pair<String, Iterable<Comparable>>>() {
                    @Nullable
                    @Override
                    public Pair<String, Iterable<Comparable>> apply(final @Nullable Row row) {
                        assert row != null;
                        InetAddress host;
                        try {
                            host = row.getInet("peer");
                        } catch (IllegalArgumentException e) {
                            host = Utils.inetAddressFromLocation(sessionWithHost.right);
                        }

                        Iterable<Comparable> sortedTokens =
                                transform(row.getSet("tokens", String.class), new Function<String, Comparable>() {
                                            @Nullable
                                            @Override
                                            public Comparable apply(final @Nullable String token) {
                                                return (Comparable) tkValidator.compose(tkValidator.fromString(token));
                                            }
                                        }
                                );

                        return Pair.create(host.getHostName(), sortedTokens);
                    }
                });

        for (Pair<String, Iterable<Comparable>> pair : pairs) {
            tokens.put(pair.left, pair.right);
        }

        return tokens;
    }

    /**
     * Merges the list of tokens for each cluster machine to a single list of token ranges.
     *
     * @param tokens      the map of tokens for each cluster machine.
     * @param session     the connection to the cluster.
     * @param partitioner the partitioner used in the cluster.
     * @param config      the Deep configuration object.
     * @return the merged lists of tokens transformed to DeepTokenRange(s). The returned collection is shuffled.
     */
    private static List<DeepTokenRange> mergeTokenRanges(Map<String, Iterable<Comparable>> tokens, final Session session,
                                                         final IPartitioner partitioner, final IDeepJobConfig config) {
        final Iterable<Comparable> allRanges = Ordering.natural().sortedCopy(concat(tokens.values()));
        final Comparable maxValue = Ordering.natural().max(allRanges);
        final Comparable minValue = (Comparable) partitioner.minValue(maxValue.getClass()).getToken().token;

        Function<Comparable, Set<DeepTokenRange>> map = new Function<Comparable, Set<DeepTokenRange>>() {
            public Set<DeepTokenRange> apply(final Comparable elem) {
                Comparable nextValue;
                Comparable currValue = elem;

                Set<DeepTokenRange> result = new HashSet<>();

                if (currValue.equals(maxValue)) {

                    result.add(new DeepTokenRange(currValue, minValue,
                            initReplicas(currValue, session, partitioner, config)));
                    currValue = minValue;

                    nextValue = Iterables.find(allRanges, new Predicate<Comparable>() {
                        @Override
                        @SuppressWarnings("unchecked")
                        public boolean apply(@Nullable Comparable input) {
                            assert input != null;
                            return input.compareTo(minValue) > 0;
                        }
                    });

                } else {

                    int nextIdx = 1 + indexOf(allRanges, new Predicate<Comparable>() {
                        @Override
                        public boolean apply(@Nullable Comparable input) {
                            assert input != null;
                            return input.equals(elem);
                        }
                    });
                    nextValue = Iterables.get(allRanges, nextIdx);
                }

                result.add(new DeepTokenRange(currValue, nextValue, initReplicas(currValue, session, partitioner, config)));

                return result;
            }
        };

        /*
        List<DeepTokenRange> result =  Lists.newArrayList(concat(transform(allRanges, map)));
        Collections.shuffle(result);
        return result;
        */
        return Ordering.natural().sortedCopy(concat(transform(allRanges, map)));
    }

    /**
     * Given a token, fetches the list of replica machines holding that token.
     *
     * @param token the token whose replicas we want to fetch.
     * @param session the connection to the cluster.
     * @param partitioner the partitioner used in the cluster.
     * @param config the Deep configuration object.
     * @return the list of replica machines holding that token.
     */
    private static List<String> initReplicas(
            final Comparable token, final Session session, final IPartitioner partitioner, final IDeepJobConfig config) {
        final AbstractType tkValidator = partitioner.getTokenValidator();
        final Metadata metadata = session.getCluster().getMetadata();

        @SuppressWarnings("unchecked")
        Set<Host> replicas = metadata.getReplicas(config.getKeyspace(), tkValidator.decompose(token));

        return Lists.newArrayList(Iterables.transform(replicas, new Function<Host, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Host input) {
                assert input != null;
                return input.getAddress().getHostName();
            }
        }));
    }

    public static List<DeepTokenRange> getSplits(IDeepJobConfig config) {
        Map<String, Iterable<Comparable>> tokens = new HashMap<>();
        IPartitioner partitioner = getPartitioner(config);

        Pair<Session, String> sessionWithHost =
                CassandraClientProvider.getSession(
                        config.getHost(),
                        config, false);

        String queryLocal = "select tokens from system.local";
        tokens.putAll(fetchTokens(queryLocal, sessionWithHost, partitioner));

        String queryPeers = "select peer, tokens from system.peers";
        tokens.putAll(fetchTokens(queryPeers, sessionWithHost, partitioner));

        return mergeTokenRanges(tokens, sessionWithHost.left, partitioner, config);
    }


    public static IPartitioner getPartitioner(IDeepJobConfig config) {
        try {
            return (IPartitioner) Class.forName(config.getPartitionerClassName()).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new DeepGenericException(e);
        }
    }

}
