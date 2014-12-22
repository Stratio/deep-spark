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

package com.stratio.deep.cassandra.cql;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.querybuilder.CassandraUpdateQueryBuilder;
import com.stratio.deep.cassandra.util.CassandraUtils;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.exception.DeepInstantiationException;
import com.stratio.deep.commons.handler.DeepRecordWriter;
import com.stratio.deep.commons.utils.Pair;

/**
 * Handles the distributed write to cassandra in batch.
 */
public final class DeepCqlRecordWriter extends DeepRecordWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DeepCqlRecordWriter.class);

    // handles for clients for each range exhausted in the threadpool
    private final Map<Token, RangeClient> clients;
    private final Map<Token, RangeClient> removedClients;

    private AbstractType<?> keyValidator;
    private String[] partitionKeyColumns;

    private final ICassandraDeepJobConfig writeConfig;
    private final IPartitioner partitioner;
    private final InetAddress localhost;

    private final CassandraUpdateQueryBuilder queryBuilder;

    private Session sessionWithHost;

    /**
     * Con
     *
     * @param writeConfig
     */
    public DeepCqlRecordWriter(ICassandraDeepJobConfig writeConfig, CassandraUpdateQueryBuilder queryBuilder) {
        this.clients = new HashMap<>();
        this.removedClients = new HashMap<>();
        this.writeConfig = writeConfig;
        this.partitioner = RangeUtils.getPartitioner(writeConfig);
        this.queryBuilder = queryBuilder;
        try {
            this.localhost = InetAddress.getLocalHost();
            sessionWithHost = CassandraClientProvider.trySessionForLocation(localhost.getHostAddress(),
                    (CassandraDeepJobConfig) writeConfig, false).left;
            sessionWithHost.init();
        } catch (UnknownHostException e) {
            throw new DeepInstantiationException("Cannot resolve local hostname", e);
        }
        init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Closing all clients");
        // close all the clients before throwing anything
        for (RangeClient client : clients.values()) {
            try {
                IOUtils.closeQuietly(client);
            } catch (Exception e) {
                LOG.error("exception", e);
            }
        }

        for (RangeClient client : removedClients.values()) {
            try {
                IOUtils.closeQuietly(client);
            } catch (Exception e) {
                LOG.error("exception", e);
            }
        }
    }

    private void init() {
        try {
            retrievePartitionKeyValidator();

        } catch (Exception e) {
            throw new DeepGenericException(e);
        }
    }

    private AbstractType<?> parseType(String type) throws ConfigurationException {
        try {
            // always treat counters like longs, specifically CCT.serialize is not what we need
            if (type != null && "org.apache.cassandra.db.marshal.CounterColumnType".equals(type)) {
                return LongType.instance;
            }
            return TypeParser.parse(type);
        } catch (SyntaxException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    /**
     * retrieve the key validator from system.schema_columnfamilies table
     */
    protected void retrievePartitionKeyValidator() throws ConfigurationException {

        String keyspace = writeConfig.getKeyspace();
        String cfName = writeConfig.getColumnFamily();

        Row row = getRowMetadata(keyspace, cfName);

        if (row == null) {
            throw new DeepIOException(String.format("cannot find metadata for %s.%s", keyspace, cfName));
        }

        String validator = row.getString("key_validator");
        keyValidator = parseType(validator);

        String keyString = row.getString("key_aliases");
        LOG.debug("partition keys: " + keyString);

        List<String> keys = FBUtilities.fromJsonList(keyString);
        partitionKeyColumns = new String[keys.size()];
        int i = 0;
        for (String key : keys) {
            partitionKeyColumns[i] = key;
            i++;
        }

        String clusterColumnString = row.getString("column_aliases");

        LOG.debug("cluster columns: " + clusterColumnString);
    }

    /**
     * Fetches row metadata for the given column family.
     *
     * @param keyspace        the keyspace name
     * @param cfName          the column family
     * @return the Row object
     */
    private Row getRowMetadata(String keyspace, String cfName) {
        String query =
                "SELECT key_validator,key_aliases,column_aliases " +
                        "FROM system.schema_columnfamilies " +
                        "WHERE keyspace_name='%s' and columnfamily_name='%s' ";
        String formatted = String.format(query, keyspace, cfName);
        ResultSet resultSet = sessionWithHost.execute(formatted);
        return resultSet.one();
    }

    /**
     * Adds the provided row to a batch. If the batch size reaches the threshold configured in
     * IDeepJobConfig.getBatchSize the batch will be sent to the data store.
     *
     * @param keys   the Cells object containing the row keys.
     * @param values the Cells object containing all the other row columns.
     */
    public void write(Cells keys, Cells values) {
        /* generate SQL */

        String localCql = queryBuilder.prepareQuery(keys, values);

        Token range = partitioner.getToken(CassandraUtils.getPartitionKey(keys, keyValidator,
                partitionKeyColumns.length));

        // add primary key columns to the bind variables
        List<Object> allValues = new ArrayList<>(values.getCellValues());
        allValues.addAll(keys.getCellValues());

        // get the client for the given range, or create a new one
        RangeClient client = clients.get(range);
        if (client == null) {
            // haven't seen keys for this range: create new client
            client = new RangeClient();
            clients.put(range, client);
        }

        if (client.put(localCql, allValues)) {
            removedClients.put(range, clients.remove(range));
        }

    }

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular range. Bound variables
     * for keys in that range are sent to this client via a queue.
     */
    protected class RangeClient extends Thread implements Closeable {

        private final int batchSize = writeConfig.getBatchSize();
        private final List<String> batchStatements = new ArrayList<>();
        private final List<Object> bindVariables = new ArrayList<>();
        private final UUID identity = UUID.randomUUID();
        private String cql;

        /**
         * Returns true if adding the current element triggers the batch execution.
         *
         * @param stmt   the query to add to the batch.
         * @param values the list of binding values.
         * @return a boolean indicating if the batch has been triggered or not.
         */
        public synchronized boolean put(String stmt, List<Object> values) {
            batchStatements.add(stmt);
            bindVariables.addAll(values);

            boolean res = batchStatements.size() >= batchSize;
            if (res) {
                triggerBatch();
            }
            return res;
        }

        /**
         * Triggers the execution of the batch
         */
        private void triggerBatch() {
            if (getState() != Thread.State.NEW) {
                return;
            }

            cql = queryBuilder.prepareBatchQuery(batchStatements);
            this.start();
        }

        /**
         * Constructs an {@link RangeClient} for the given endpoints.
         */
        public RangeClient() {
            LOG.debug("Create client " + this);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return identity.toString();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            LOG.debug("[" + this + "] Called close on client, state: " + this.getState());
            triggerBatch();

            try {
                this.join();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        /**
         * Loops collecting cql binded variable values from the queue and sending to Cassandra
         */
        @Override
        public void run() {
            LOG.debug("[" + this + "] Initializing cassandra client");
            sessionWithHost.execute(cql, bindVariables.toArray(new Object[bindVariables.size()]));
        }
    }
}
