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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.exception.DeepGenericException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Class that provides several token range utilities using Cassandra's Thrift RPC API.
 *
 * @author Andres de la Pena <andres@stratio.com>
 */
public class ThriftRangeUtils
{
    private final String host; // The Cassandra contact host name
    private final int rpcPort; // The Cassandra contact host RPC port
    private final int splitSize; // The number of rows per split
    private final String keyspace; // The Cassandra keyspace name
    private final String columnFamily; // The Cassandra column family name
    private final AbstractType tokenValidator; // The token validator/type
    private final TokenFactory tokenFactory; // The token factory

    /**
     * Builds a new {@link com.stratio.deep.cql.ThriftRangeUtils}.
     *
     * @param partitioner
     *            the Cassandra partitioner.
     * @param host
     *            the Cassandra host address.
     * @param rpcPort
     *            the Cassandra host RPC port.
     * @param splitSize
     *            the number of rows per split.
     * @param keyspace
     *            the Cassandra keyspace name.
     * @param columnFamily
     *            the Cassandra column family name.
     */
    public ThriftRangeUtils(IPartitioner partitioner,
                            String host,
                            int rpcPort,
                            int splitSize,
                            String keyspace,
                            String columnFamily)
    {
        this.host = host;
        this.rpcPort = rpcPort;
        this.splitSize = splitSize;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        tokenValidator = partitioner.getTokenValidator();
        tokenFactory = partitioner.getTokenFactory();
    }

    /**
     * Returns a new {@link com.stratio.deep.cql.ThriftRangeUtils} using the specified configuration.
     *
     * @param config
     *            the Deep configuration object.
     */
    public static ThriftRangeUtils build(ICassandraDeepJobConfig config) {
        String host = config.getHost();
        int rpcPort = config.getRpcPort();
        int splitSize = config.getSplitSize();
        String keyspace = config.getKeyspace();
        String columnFamily = config.getColumnFamily();
        IPartitioner partitioner;
        try {
            partitioner = (IPartitioner) Class.forName(config.getPartitionerClassName()).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new DeepGenericException(e);
        }
        return new ThriftRangeUtils(partitioner, host, rpcPort, splitSize, keyspace, columnFamily);
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
            List<DeepTokenRange> nodeSplits = getSplits( tokenRange);
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

            TTransport transport = new TFramedTransport(new TSocket(host, rpcPort));
            TProtocol protocol = new TBinaryProtocol(transport);
            Client client = new Cassandra.Client(protocol);
            transport.open();

            // List<TokenRange> tokenRanges = client.describe_local_ring(config.getKeyspace());
            List<TokenRange> tokenRanges = client.describe_ring(keyspace);

            List<DeepTokenRange> deepTokenRanges = new ArrayList<>(tokenRanges.size());
            for (TokenRange tokenRange : tokenRanges) {
                Comparable start = tokenAsComparable(tokenRange.getStart_token());
                Comparable end = tokenAsComparable(tokenRange.getEnd_token());
                deepTokenRanges.add(new DeepTokenRange(start, end, tokenRange.getEndpoints()));
            }

            transport.close();

            return deepTokenRanges;
        } catch (TException e) {
            throw new DeepGenericException("No available replicas for get token ranges", e);
        }
    }

    /**
     * Returns the computed token range splits of the specified token range.
     *
     * @param deepTokenRange
     *            the token range to be splitted.
     * @return the list of token range splits, which are also token ranges.
     */
    public List<DeepTokenRange> getSplits(DeepTokenRange deepTokenRange) {

        String start = tokenAsString(deepTokenRange.getStartToken());
        String end = tokenAsString(deepTokenRange.getEndToken());

        List<String> endpoints = deepTokenRange.getReplicas();

        List<DeepTokenRange> result = new ArrayList<>();
        for (String endpoint : endpoints) {
            try
            {
                TTransport transport = new TFramedTransport(new TSocket(endpoint, rpcPort));
                TProtocol protocol = new TBinaryProtocol(transport);
                Client client = new Cassandra.Client(protocol);
                transport.open();
                client.set_keyspace(keyspace);

                List<CfSplit> splits = client.describe_splits_ex(columnFamily, start, end, splitSize);
                transport.close();
                for (CfSplit split : splits) {
                    Comparable splitStart = tokenAsComparable(split.getStart_token());
                    Comparable splitEnd = tokenAsComparable(split.getEnd_token());
                    result.add(new DeepTokenRange(splitStart, splitEnd, endpoints));
                }

                return result;
            } catch (TException e) {
                // Nothing to do
            }
        }
        throw new DeepGenericException("No available replicas for split token range: " + deepTokenRange);
    }

    /**
     * Returns the specified token as a {@link java.lang.Comparable}.
     *
     * @param tokenAsString
     *            a token represented as a {@link java.lang.String}.
     * @return the specified token as a {@link java.lang.Comparable}.
     */
    public Comparable tokenAsComparable(String tokenAsString) {
        Token token = tokenFactory.fromString(tokenAsString);
        ByteBuffer bb = tokenFactory.toByteArray(token);
        return (Comparable) tokenValidator.compose(bb);
    }

    /**
     * Returns the specified token as a {@link java.lang.String}.
     *
     * @param tokenAsComparable
     *            a token represented as a {@link java.lang.Comparable}.
     * @return the specified token as a {@link java.lang.String}.
     */
    public String tokenAsString(Comparable tokenAsComparable) {
        ByteBuffer bb = tokenValidator.decompose(tokenAsComparable);
        Token token = tokenFactory.fromByteArray(bb);
        return tokenFactory.toString(token);
    }

}
