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
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.cassandra.utils.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Iterables.transform;

/**
 * {@link CqlPagingRecordReader} implementation that returns an instance of a
 * {@link DeepRecordReader}.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class RangeUtils<T> {

    private RangeUtils() {
    }

    private static Map<String, List<Comparable>> fetchSortedTokens(
            String query, final Pair<Session, InetAddress> sessionWithHost, IPartitioner partitioner) {

        ResultSet rSet = sessionWithHost.left.execute(query);

        final AbstractType tkValidator = partitioner.getTokenValidator();
        final Map<String, List<Comparable>> tokens = Maps.newHashMap();

        Iterable<Pair<String, List<Comparable>>> pairs =
                transform(rSet.all(), new Function<Row, Pair<String, List<Comparable>>>() {
                    @Nullable
                    @Override
                    public Pair<String, List<Comparable>> apply(final @Nullable Row row) {
                        assert row != null;
                        InetAddress host;
                        try {
                            host = row.getInet("peer");
                        } catch (IllegalArgumentException e) {
                            host = sessionWithHost.right;
                        }

                        List<Comparable> sortedTokens = Ordering.natural().immutableSortedCopy(
                                transform(row.getSet("tokens", String.class), new Function<String, Comparable>() {
                                    @Nullable
                                    @Override
                                    public Comparable apply(final @Nullable String token) {
                                        return (Comparable) tkValidator.compose(tkValidator.fromString(token));
                                    }
                                })
                        );

                        return Pair.create(host.getHostName(), sortedTokens);
                    }
                });

        for (Pair<String, List<Comparable>> pair : pairs) {
            tokens.put(pair.left, pair.right);
        }

        return tokens;
    }

    private static List<DeepTokenRange> mergeTokenRanges(Map<String, List<Comparable>> tokens, final Session session,
                                                         final IPartitioner partitioner, final IDeepJobConfig config) {
        final Iterable<Comparable> allRanges = Ordering.natural().sortedCopy(concat(tokens.values()));
        final Comparable maxValue = Ordering.natural().max(allRanges);
        final Comparable minValue = (Comparable) partitioner.minValue(maxValue.getClass()).getToken().token;

        Function<Comparable, List<DeepTokenRange>> map = new Function<Comparable, List<DeepTokenRange>>() {
            public List<DeepTokenRange> apply(final Comparable elem) {
                Comparable nextValue;
                Comparable currValue = elem;

                List<DeepTokenRange> result = new ArrayList<>();

                if (currValue.equals(maxValue)) {

                    result.add(new DeepTokenRange(currValue, minValue,
                            initReplicas(currValue, session, partitioner, config)));
                    currValue = minValue;
                    nextValue = Iterables.getFirst(allRanges, null);

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

                result.add(new DeepTokenRange(currValue, nextValue,initReplicas(currValue, session, partitioner, config)));

                return result;
            }
        };

        return Ordering.natural().sortedCopy(concat(transform(allRanges, map)));
    }

    private static String[] initReplicas(
            final Comparable startToken, final Session session, final IPartitioner partitioner, final IDeepJobConfig config){
        final AbstractType tkValidator = partitioner.getTokenValidator();
        final Metadata metadata = session.getCluster().getMetadata();

        Set<Host> replicas =
                metadata.getReplicas(
                        config.getKeyspace(),
                        tkValidator.decompose(startToken));

        return Iterables.toArray(Iterables.transform(replicas, new Function<Host, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Host input) {
                assert input != null;
                return input.getAddress().getHostName();
            }
        }), String.class);
    }

    public static List<DeepTokenRange> getSplits(IDeepJobConfig config) {
        Map<String, List<Comparable>> tokens = new HashMap<>();
        IPartitioner partitioner = getPartitioner(config);

        Pair<Session, InetAddress> sessionWithHost =
                CassandraClientProvider.getUnbalancedSession(
                config.getHost(),
                config.getCqlPort(),
                config.getKeyspace());


        try (Session session = sessionWithHost.left) {
            String queryLocal = "select tokens from system.local";
            tokens.putAll(fetchSortedTokens(queryLocal, sessionWithHost, partitioner));

            String queryPeers = "select peer, tokens from system.peers";
            tokens.putAll(fetchSortedTokens(queryPeers, sessionWithHost, partitioner));

            return mergeTokenRanges(tokens, session, partitioner, config);

            /*
            List<DeepTokenRange> hadoopTr = new ArrayList<>();
            for (InputSplit split : splits) {
                Long startToken = Long.parseLong(((ColumnFamilySplit) split).getStartToken());
                Long endToken = Long.parseLong(((ColumnFamilySplit) split).getEndToken());
                DeepTokenRange finder = new DeepTokenRange(startToken, endToken);
                hadoopTr.add(finder);
            }


            boolean elementsEquals = Iterables.elementsEqual(
                    Ordering.natural().sortedCopy(finalTokenRanges),
                    Ordering.natural().sortedCopy(hadoopTr));
            System.out.println("elementsEquals: "+elementsEquals);
            */
        }

    }

    public static IPartitioner getPartitioner(IDeepJobConfig config) {
        try {
            return (IPartitioner) Class.forName(config.getPartitionerClassName()).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new DeepGenericException(e);
        }
    }

}
