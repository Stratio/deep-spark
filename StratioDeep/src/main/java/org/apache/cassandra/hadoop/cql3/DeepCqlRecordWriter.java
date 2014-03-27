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

package org.apache.cassandra.hadoop.cql3;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.cql.DeepConfigHelper;
import com.stratio.deep.testentity.Cell;
import com.stratio.deep.testentity.Cells;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.testutils.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.AbstractColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.AbstractColumnFamilyRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.Progressable;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import static com.stratio.deep.testutils.Utils.updateQueryGenerator;

/**
 * Created by luca on 05/02/14.
 */
public class DeepCqlRecordWriter extends AbstractColumnFamilyRecordWriter<Cells, Cells> {
    private static final Logger LOG = LoggerFactory.getLogger(DeepCqlRecordWriter.class);

    // handles for clients for each range running in the threadpool
    private final Map<Range<?>, RangeClient> clients;

    // host to prepared statement id mappings
    private ConcurrentHashMap<Cassandra.Client, Integer> preparedStatements = new ConcurrentHashMap<>();
    private AbstractType<?> keyValidator;
    private String[] partitionKeyColumns;

    private List<String> clusterColumns;

    private String cql;

    DeepCqlRecordWriter(Configuration conf) {
        super(conf);
        this.clients = new HashMap<>();
        init(conf);
    }

    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     *
     * @param context the task attempt context
     * @throws IOException
     */
    DeepCqlRecordWriter(TaskAttemptContext context) {
        this(context.getConfiguration());
        this.progressable = new Progressable(context);
    }

    @Override
    public void close() throws IOException {
        // close all the clients before throwing anything
        for (RangeClient client : clients.values()) {
            IOUtils.closeQuietly(client);
        }
    }

    protected ByteBuffer getPartitionKey(Cells cells) {
        ByteBuffer partitionKey;
        if (keyValidator instanceof CompositeType) {
            ByteBuffer[] keys = new ByteBuffer[partitionKeyColumns.length];

            for (int i = 0; i < cells.size(); i++) {
                Cell<?> c = cells.getCellByIdx(i);

                if (c.isPartitionKey()) {
                    keys[i] = c.getDecomposedCellValue();
                }
            }

            partitionKey = CompositeType.build(keys);
        } else {
            partitionKey = cells.getCellByIdx(0).getDecomposedCellValue();
        }
        return partitionKey;
    }

    protected void init(Configuration conf) {
        try {
            Cassandra.Client client = ConfigHelper.getClientFromOutputAddressList(conf);
            retrievePartitionKeyValidator(client);

            TTransport transport = client.getOutputProtocol().getTransport();
            if (transport.isOpen()) {
                transport.close();
            }

        } catch (Exception e) {
            throw new DeepGenericException(e);
        }
    }

    protected AbstractType<?> parseType(String type) throws ConfigurationException {
        try {
            // always treat counters like longs, specifically CCT.serialize is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType")) {
                return LongType.instance;
            }
            return TypeParser.parse(type);
        } catch (SyntaxException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    /**
     * Quoting for working with uppercase
     */
    protected String quote(String identifier) {
        StringBuilder sb = new StringBuilder("\"").append(identifier.replaceAll("\"", "\"\"")).append("\"");
        return sb.toString();
    }

    /**
     * retrieve the key validator from system.schema_columnfamilies table
     */
    protected void retrievePartitionKeyValidator(Cassandra.Client client) throws Exception {
        String keyspace = ConfigHelper.getOutputKeyspace(conf);
        String cfName = ConfigHelper.getOutputColumnFamily(conf);

        String query = "SELECT key_validator," + "       key_aliases," + "       column_aliases "
            + "FROM system.schema_columnfamilies " + "WHERE keyspace_name='%s' and columnfamily_name='%s'";
        String formatted = String.format(query, keyspace, cfName);
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(formatted), Compression.NONE,
            ConsistencyLevel.ONE);

        Column rawKeyValidator = result.rows.get(0).columns.get(0);
        String validator = ByteBufferUtil.string(ByteBuffer.wrap(rawKeyValidator.getValue()));
        keyValidator = parseType(validator);

        Column rawPartitionKeys = result.rows.get(0).columns.get(1);
        String keyString = ByteBufferUtil.string(ByteBuffer.wrap(rawPartitionKeys.getValue()));
        LOG.debug("partition keys: " + keyString);

        List<String> keys = FBUtilities.fromJsonList(keyString);
        partitionKeyColumns = new String[keys.size()];
        int i = 0;
        for (String key : keys) {
            partitionKeyColumns[i] = key;
            i++;
        }

        Column rawClusterColumns = result.rows.get(0).columns.get(2);
        String clusterColumnString = ByteBufferUtil.string(ByteBuffer.wrap(rawClusterColumns.getValue()));

        LOG.debug("cluster columns: " + clusterColumnString);
        clusterColumns = FBUtilities.fromJsonList(clusterColumnString);
    }

    /**
     * If the key is to be associated with a valid value, a mutation is created
     * for it with the given column family and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link org.apache.cassandra.thrift.Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link org.apache.cassandra.thrift.Deletion}.
     * </p>
     *
     * @throws IOException 13
     */
    @Override
    public void write(Cells keys, Cells values) throws IOException {
        /* generate SQL */
        String localCql = updateQueryGenerator(keys, values, ConfigHelper.getOutputKeyspace(conf),
            ConfigHelper.getOutputColumnFamily(conf));

        @SuppressWarnings("rawtypes")
        Range<Token> range = ringCache.getRange(getPartitionKey(keys));

        // add primary key columns to the bind variables
        List<ByteBuffer> allValues = new ArrayList<>(values.getDecomposedCellValues());
        allValues.addAll(keys.getDecomposedCellValues());

        // get the client for the given range, or create a new one
        synchronized (clients) {
            RangeClient client = clients.get(range);
            if (client == null) {
                // haven't seen keys for this range: create new client
                client = new RangeClient(ringCache.getEndpoint(range));
                clients.put(range, client);
            }

            if (client.put(localCql, allValues)) {
                clients.remove(range);
            }
        }

        progressable.progress();
    }

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Bound variables for keys in that range are sent to this client via a queue.
     */
    private class RangeClient extends AbstractRangeClient<List<ByteBuffer>> implements Closeable {

        private final int batchSize = DeepConfigHelper.getOutputBatchSize(conf);
        private List<String> batchStatements = new ArrayList<>();
        private List<ByteBuffer> bindVariables = new ArrayList<>();

        /**
         * Returns true if adding the current element triggers the batch execution.
         *
         * @param stmt
         * @param values
         * @return
         * @throws IOException
         */
        public boolean put(String stmt, List<ByteBuffer> values) throws IOException {
            batchStatements.add(stmt);
            bindVariables.addAll(values);

            boolean res = batchStatements.size() >= batchSize;
            if (res) {
                triggerBatch();
            }
            return res;
        }

        private void triggerBatch() throws IOException {
            cql = Utils.batchQueryGenerator(batchStatements);
            LOG.debug("Executing query: " + cql);
            this.start();
        }

        /**
         * Constructs an {@link RangeClient} for the given endpoints.
         *
         * @param endpoints the possible endpoints to execute the mutations on
         */
        public RangeClient(List<InetAddress> endpoints) {
            super(endpoints);
        }

        @Override
        public void close() throws IOException {
            triggerBatch();
            try {
                this.join();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }

            if (lastException != null) {
                throw lastException;
            }
        }

        /**
         * get prepared statement id from cache, otherwise prepare it from Cassandra server
         */
        private Integer preparedStatement(Cassandra.Client client) {
            Integer itemId = preparedStatements.get(client);
            if (itemId == null) {
                CqlPreparedResult result;
                try {
                    result = client.prepare_cql3_query(ByteBufferUtil.bytes(cql), Compression.NONE);
                } catch (TException e) {
                    throw new DeepGenericException("failed to prepare cql query " + cql, e);
                }

                Integer previousId = preparedStatements.putIfAbsent(client, result.itemId);
                itemId = previousId == null ? result.itemId : previousId;
            }
            return itemId;
        }

        /**
         * Loops collecting cql binded variable values from the queue and sending to Cassandra
         */
        @Override
        public void run() {
            Iterator<InetAddress> iter = endpoints.iterator();
            while (true) {
                // send the mutation to the last-used endpoint.  first time through, this will NPE harmlessly.
                try {
                    int itemId = preparedStatement(client);
                    client.execute_prepared_cql3_query(itemId, bindVariables, ConsistencyLevel.ONE);

                    break;
                } catch (Exception e) {
                    closeInternal();

                    if (!iter.hasNext()) {
                        lastException = new IOException(e);
                        continue;
                    }
                }

                // attempt to connect to a different endpoint
                try {
                    InetAddress address = iter.next();
                    String host = address.getHostName();
                    int port = ConfigHelper.getOutputRpcPort(conf);
                    client = AbstractColumnFamilyOutputFormat.createAuthenticatedClient(host, port, conf);
                } catch (Exception e) {
                    closeInternal();
                    // TException means something unexpected went wrong to that endpoint, so
                    // we should try again to another.  Other exceptions (auth or invalid request) are fatal.
                    if ((!(e instanceof TException)) || !iter.hasNext()) {
                        lastException = new IOException(e);
                        continue;
                    }
                }
            }
        }
    }
}
