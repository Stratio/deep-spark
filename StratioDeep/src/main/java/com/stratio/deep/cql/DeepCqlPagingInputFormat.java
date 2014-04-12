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
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepGenericException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static java.util.Collections.sort;

/**
 * {@link CqlPagingRecordReader} implementation that returns an instance of a
 * {@link DeepCqlPagingRecordReader}.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepCqlPagingInputFormat<T> extends CqlPagingInputFormat {
    private final IDeepJobConfig<T> deepJobConfig;
    private final IPartitioner partitioner;

    public DeepCqlPagingInputFormat(IDeepJobConfig<T> deepJobConfig) {
        this.deepJobConfig = deepJobConfig;
        this.partitioner = getPartitioner();
    }

    /**
     * Returns a new instance of {@link DeepCqlPagingRecordReader}.
     */
    public RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>> createRecordReader(InputSplit arg0,
                                                                                             DeepTaskAttemptContext arg1) throws IOException, InterruptedException {

        return new DeepCqlPagingRecordReader(arg1.getConf().getAdditionalFilters(), deepJobConfig);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = super.getSplits(context);

        Session session = CassandraClientProvider.getSession(deepJobConfig.getHost(), deepJobConfig.getCqlPort(), "system");

        String queryLocal = "select tokens from system.local";

        ResultSet resultSet = session.execute(queryLocal);
        Row localRecord = resultSet.one();

        Map<String, List<Comparable>> tokens = new HashMap<>();
        Set<String> unsortedTokens = localRecord.getSet("tokens", String.class);

        AbstractType tkValidator = partitioner.getTokenValidator();

        for (String token : unsortedTokens) {
            Comparable convertedToken = (Comparable) tkValidator.compose(tkValidator.fromString(token));

            List<Comparable> sortedTokens = tokens.get(deepJobConfig.getHost());
            if (sortedTokens == null){
                sortedTokens = new ArrayList<>();
                tokens.put(deepJobConfig.getHost(), sortedTokens);
            }

            sortedTokens.add(convertedToken);
        }

        Collections.sort(tokens.get(deepJobConfig.getHost()));

        String queryPeers = "select peer, tokens from system.peers";

        ResultSet peersResultSet = session.execute(queryPeers);

        for (Row peersRecord : peersResultSet) {
            InetAddress host = peersRecord.getInet("peer");

            unsortedTokens = peersRecord.getSet("tokens", String.class);

            for (String token : unsortedTokens) {
                Comparable convertedToken = (Comparable) tkValidator.compose(tkValidator.fromString(token));

                List<Comparable> sortedTokens = tokens.get(host.getHostName());
                if (sortedTokens == null){
                    sortedTokens = new ArrayList<>();
                    tokens.put(host.getHostName(), sortedTokens);
                }

                sortedTokens.add(convertedToken);
            }

            Collections.sort(tokens.get(host.getHostName()));
        }

        List<Comparable> allRanges = new ArrayList<>();

        for (Map.Entry<String, List<Comparable>> entry : tokens.entrySet()) {
            allRanges.addAll(entry.getValue());
        }
        Collections.sort(allRanges);

        Metadata metadata = session.getCluster().getMetadata();

        System.out.println("Final token ranges:\n");
        List<TokenRange> finalTokenRanges = new ArrayList<>();
        for (int idx = 0; idx < allRanges.size(); idx++) {
            int nextIdx = idx + 1;
            Comparable currValue = allRanges.get(idx);
            Comparable nextValue = null;

            if (nextIdx == allRanges.size()){
                nextIdx = 0;
                nextValue =
                        (Comparable) partitioner.minValue(allRanges.get(nextIdx).getClass()).getToken().token;
                TokenRange tr = new TokenRange(currValue, nextValue);
                tr.setReplicas(metadata.getReplicas(deepJobConfig.getKeyspace(), tkValidator.decompose(tr.startToken)));
                finalTokenRanges.add(tr);
                currValue = nextValue;
            }
            nextValue = allRanges.get(nextIdx);

            TokenRange tr = new TokenRange(currValue, nextValue);
            tr.setReplicas(metadata.getReplicas(deepJobConfig.getKeyspace(), tkValidator.decompose(tr.startToken)));
            finalTokenRanges.add(tr);


        }
        Collections.sort(finalTokenRanges);
        for (TokenRange tr : finalTokenRanges) {
            System.out.println(tr);
        }

        int k = 0;
        System.out.println("Hadoop token ranges:\n");
        List<TokenRange> hadoopTr = new ArrayList<>();
        for (InputSplit split : splits) {
            Long startToken = Long.parseLong(((ColumnFamilySplit)split).getStartToken());
            Long endToken = Long.parseLong(((ColumnFamilySplit)split).getEndToken());
            TokenRange finder = new TokenRange(startToken, endToken);
            hadoopTr.add(finder);
            if (finalTokenRanges.contains(finder)){


                k++;
            }

        }
        Collections.sort(hadoopTr);
        for (TokenRange tr : hadoopTr) {
            System.out.println(tr);
        }

        System.out.print("Found " + k + " ranges");

        return splits;
    }

    private IPartitioner getPartitioner() {
        try {
            return (IPartitioner) Class.forName(deepJobConfig.getPartitionerClassName()).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new DeepGenericException(e);
        }
    }

    class TokenRange implements Comparable<TokenRange> {
        private Comparable startToken;
        private Comparable endToken;
        private Set<Host> replicas;

        TokenRange(Comparable startToken, Comparable endToken) {
            this.startToken = startToken;
            this.endToken = endToken;
        }

        @Override
        public String toString() {
            return "{" +
                    "start=" + startToken +
                    ", end=" + endToken +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TokenRange that = (TokenRange) o;

            if (!endToken.equals(that.endToken)) return false;
            if (!startToken.equals(that.startToken)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = startToken.hashCode();
            result = 31 * result + endToken.hashCode();
            return result;
        }

        public Comparable getStartToken() {
            return startToken;
        }

        public void setStartToken(Comparable startToken) {
            this.startToken = startToken;
        }

        public Comparable getEndToken() {
            return endToken;
        }

        public void setEndToken(Comparable endToken) {
            this.endToken = endToken;
        }

        public Set<Host> getReplicas() {
            return replicas;
        }

        public void setReplicas(Set<Host> replicas) {
            this.replicas = replicas;
        }

        @Override
        public int compareTo(TokenRange o) {
            return startToken.compareTo(o.startToken);
        }
    }
}
