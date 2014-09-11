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

package com.stratio.deep.thrift;

import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.cql.DeepTokenRange;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.utils.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that provides several token range utilities using Cassandra's Thrift RPC API.
 *
 * @author Andres de la Pena <andres@stratio.com>
 */
public class ThriftRangeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftRangeUtils.class);

    private final String host; // The Cassandra contact host name
    private final int rpcPort; // The Cassandra contact host RPC port
    private final int splitSize; // The number of rows per split
    private final String keyspace; // The Cassandra keyspace name
    private final String columnFamily; // The Cassandra column family name
    private final AbstractType tokenType; // The token validator
    private final TokenFactory tokenFactory; // The token factory
    private final Comparable minToken; // The partitioner's minimum token

    /**
     * Builds a new {@link ThriftRangeUtils}.
     *
     * @param partitioner  the partitioner.
     * @param host         the host address.
     * @param rpcPort      the host RPC port.
     * @param keyspace     the keyspace name.
     * @param columnFamily the column family name.
     * @param splitSize    the number of rows per split.
     */
    ThriftRangeUtils(IPartitioner partitioner,
                     String host,
                     int rpcPort,
                     String keyspace,
                     String columnFamily,
                     int splitSize) {
        this.host = host;
        this.rpcPort = rpcPort;
        this.splitSize = splitSize;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        tokenType = partitioner.getTokenValidator();
        tokenFactory = partitioner.getTokenFactory();
        minToken = (Comparable) partitioner.getMinimumToken().token;
    }

    /**
     * Returns a new {@link ThriftRangeUtils} using the specified configuration.
     *
     * @param config the Deep configuration object.
     */
    public static ThriftRangeUtils build(ICassandraDeepJobConfig config) {
        String host = config.getHost();
        int rpcPort = config.getRpcPort();
        int splitSize = config.getSplitSize();
        String keyspace = config.getKeyspace();
        String columnFamily = config.getColumnFamily();
        String partitionerClassName = config.getPartitionerClassName();
        IPartitioner partitioner = Utils.newTypeInstance(partitionerClassName, IPartitioner.class);
        return new ThriftRangeUtils(partitioner, host, rpcPort, keyspace, columnFamily, splitSize);
    }

    /**
     * Returns the token range splits of the Cassandra ring that will be mapped to Spark partitions.
     *
     * @return the list of computed token ranges.
     */
    public List<DeepTokenRange> getSplits() {

        // Get the cluster token ranges
        List<DeepTokenRange> tokenRanges = getRanges();

        // Get the cluster token ranges splits
        List<DeepTokenRange> splits = new ArrayList<>();
        for (DeepTokenRange tokenRange : tokenRanges) {
            List<DeepTokenRange> nodeSplits = getSplits(tokenRange);
            splits.addAll(nodeSplits);
        }

        return splits;
    }

    /**
     * Returns the token ranges of the Cassandra ring that will be mapped to Spark partitions.
     * The returned ranges are the Cassandra's physical ones, without any splitting.
     *
     * @return the list of Cassandra ring token ranges.
     */
    public List<DeepTokenRange> getRanges() {
        try {

            List<TokenRange> tokenRanges;
            ThriftClient client = ThriftClient.build(host, rpcPort);
            try {
                tokenRanges = client.describe_local_ring(keyspace);
            } catch (TApplicationException e) {
                if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                    tokenRanges = client.describe_ring(keyspace);
                } else {
                    throw new DeepGenericException("Unknown server error", e);
                }
            }
            client.close();

            List<DeepTokenRange> deepTokenRanges = new ArrayList<>(tokenRanges.size());
            for (TokenRange tokenRange : tokenRanges) {
                Comparable start = tokenAsComparable(tokenRange.getStart_token());
                Comparable end = tokenAsComparable(tokenRange.getEnd_token());
                deepTokenRanges.add(new DeepTokenRange(start, end, tokenRange.getEndpoints()));
            }

            return deepTokenRanges;
        } catch (TException e) {
            throw new DeepGenericException("No available replicas for get ring token ranges", e);
        }
    }

    /**
     * Returns the computed token range splits of the specified token range.
     *
     * @param deepTokenRange the token range to be splitted.
     * @return the list of token range splits, which are also token ranges.
     */
    public List<DeepTokenRange> getSplits(DeepTokenRange deepTokenRange) {

        String start = tokenAsString(deepTokenRange.getStartToken());
        String end = tokenAsString(deepTokenRange.getEndToken());
        List<String> endpoints = deepTokenRange.getReplicas();

        for (String endpoint : endpoints) {
            try {
                ThriftClient client = ThriftClient.build(endpoint, rpcPort, keyspace);
                List<CfSplit> splits = client.describe_splits_ex(columnFamily, start, end, splitSize);
                client.close();

                List<DeepTokenRange> result = new ArrayList<>();
                for (CfSplit split : splits) {
                    Comparable splitStart = tokenAsComparable(split.getStart_token());
                    Comparable splitEnd = tokenAsComparable(split.getEnd_token());
                    if (splitStart.equals(splitEnd)) {
                        result.add(new DeepTokenRange(minToken, minToken, endpoints));
                    } else if (splitStart.compareTo(splitEnd) > 0) {
                        result.add(new DeepTokenRange(splitStart, minToken, endpoints));
                        result.add(new DeepTokenRange(minToken, splitEnd, endpoints));
                    } else {
                        result.add(new DeepTokenRange(splitStart, splitEnd, endpoints));
                    }
                }
                return result;
            } catch (TException e) {
                LOG.warn("Endpoint %s failed while splitting range %s", endpoint, deepTokenRange);
            }
        }
        throw new DeepGenericException("No available replicas for splitting range " + deepTokenRange);
    }

    /**
     * Returns the specified token as a {@link java.lang.Comparable}.
     *
     * @param tokenAsString a token represented as a {@link java.lang.String}.
     * @return the specified token as a {@link java.lang.Comparable}.
     */
    @SuppressWarnings("unchecked")
    public Comparable tokenAsComparable(String tokenAsString) {
        Token token = tokenFactory.fromString(tokenAsString);
        ByteBuffer bb = tokenFactory.toByteArray(token);
        return (Comparable) tokenType.compose(bb);
    }

    /**
     * Returns the specified token as a {@link java.lang.String}.
     *
     * @param tokenAsComparable a token represented as a {@link java.lang.Comparable}.
     * @return the specified token as a {@link java.lang.String}.
     */
    @SuppressWarnings("unchecked")
    public String tokenAsString(Comparable tokenAsComparable) {
        ByteBuffer bb = tokenType.decompose(tokenAsComparable);
        Token token = tokenFactory.fromByteArray(bb);
        return tokenFactory.toString(token);
    }

}
