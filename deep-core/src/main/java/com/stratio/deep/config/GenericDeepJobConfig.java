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

package com.stratio.deep.config;

import com.datastax.driver.core.*;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.exception.DeepIndexNotFoundException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.utils.Constants;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.stratio.deep.utils.Utils.createTableQueryGenerator;
import static com.stratio.deep.utils.Utils.quote;

/**
 * Base class for all config implementations providing default implementations for methods
 * defined in {@link com.stratio.deep.config.IDeepJobConfig}.
 */
public abstract class GenericDeepJobConfig<T> implements ICassandraDeepJobConfig<T>, AutoCloseable {
    private static final Logger LOG = Logger.getLogger("com.stratio.deep.config.GenericDeepJobConfig");
    private static final long serialVersionUID = -7179376653643603038L;
    private String partitionerClassName = "org.apache.cassandra.dht.Murmur3Partitioner";

    /**
     * keyspace name
     */
    private String keyspace;

    /**
     * name of the columnFamily from which data will be fetched
     */
    private String columnFamily;

    /**
     * hostname of the cassandra server
     */
    private String host;

    /**
     * Cassandra server RPC port.
     */
    private Integer rpcPort = Constants.DEFAULT_CASSANDRA_RPC_PORT;

    /**
     * Cassandra server CQL port.
     */
    private Integer cqlPort = Constants.DEFAULT_CASSANDRA_CQL_PORT;

    /**
     * Cassandra username. Leave empty if you do not need authentication.
     */
    private String username;

    /**
     * Cassandra password. Leave empty if you do not need authentication.
     */
    private String password;

    /**
     * default "where" filter to use to access ColumnFamily's data.
     */
    private Map<String, Serializable> additionalFilters = new TreeMap<>();

    /**
     * Defines a projection over the CF columns.
     */
    private String[] inputColumns;

    /**
     * Size of the batch created when writing to Cassandra.
     */
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;

    /**
     * holds columns metadata fetched from Cassandra.
     */
    private transient Map<String, Cell> columnDefinitionMap;

    /**
     * Default read consistency level. Defaults to LOCAL_ONE.
     */
    private String readConsistencyLevel = ConsistencyLevel.LOCAL_ONE.name();

    /**
     * Default write consistency level. Defaults to QUORUM.
     */
    private String writeConsistencyLevel = ConsistencyLevel.QUORUM.name();

    /**
     * Enables/Disables auto-creation of column family when writing to Cassandra.
     * By Default we do not create the output column family.
     */
    private Boolean createTableOnWrite = Boolean.FALSE;

    private transient Session session;

    private Boolean isInitialized = Boolean.FALSE;

    private int pageSize = Constants.DEFAULT_PAGE_SIZE;

    protected Boolean isWriteConfig = Boolean.FALSE;

    private int bisectFactor = Constants.DEFAULT_BISECT_FACTOR;

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> session(Session session) {
        this.session = session;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Session getSession() {
        if (session == null) {
            Cluster cluster = Cluster.builder()
                    .withPort(this.cqlPort)
                    .addContactPoint(this.host)
                    .withCredentials(this.username, this.password)
                    .build();

            session = cluster.connect(this.keyspace);
        }

        return session;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("closing " + getClass().getCanonicalName());
        if (session != null) {
            session.close();
        }
    }

    /**
     * {@inheritDoc}

     @Override protected void finalize() {
     LOG.debug("finalizing " + getClass().getCanonicalName());
     close();
     }
     */
    /**
     * Checks if this configuration object has been initialized or not.
     *
     * @throws com.stratio.deep.exception.DeepIllegalAccessException if not initialized
     */
    protected void checkInitialized() {
        if (!isInitialized) {
            throw new DeepIllegalAccessException("DeepJobConfig has not been initialized!");
        }
    }

    /**
     * Fetches table metadata from the underlying datastore, using DataStax java driver.
     *
     * @return the table metadata as returned by the driver.
     */
    public TableMetadata fetchTableMetadata() {

        Metadata metadata = getSession().getCluster().getMetadata();
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(this.keyspace);

        if (ksMetadata != null) {
            return ksMetadata.getTable(quote(this.columnFamily));
        } else {
            return null;
        }
    }

    /**
     * Creates the output column family if not exists. <br/>
     * We first check if the column family exists. <br/>
     * If not, we get the first element from <i>tupleRDD</i> and we use it as a template to get columns metadata.
     * <p>
     * This is a very heavy operation since to obtain the schema
     * we need to get at least one element of the output RDD.
     * </p>
     *
     * @param tupleRDD the pair RDD.
     */
    public void createOutputTableIfNeeded(RDD<Tuple2<Cells, Cells>> tupleRDD) {

        TableMetadata metadata = getSession().getCluster().getMetadata().getKeyspace(this.keyspace).getTable(this
                .columnFamily);

        if (metadata == null && !createTableOnWrite) {
            throw new DeepIOException("Cannot write RDD, output table does not exists and configuration object has " +
                    "'createTableOnWrite' = false");
        }

        if (metadata != null) {
            return;
        }

        Tuple2<Cells, Cells> first = tupleRDD.first();

        if (first._1() == null || first._1().isEmpty()) {
            throw new DeepNoSuchFieldException("no key structure found on row metadata");
        }
        String createTableQuery = createTableQueryGenerator(first._1(), first._2(), getKeyspace(), getColumnFamily());
        getSession().execute(createTableQuery);
        waitForNewTableMetadata();
    }

    /**
     * waits until table metadata is not null
     */
    private void waitForNewTableMetadata() {
        TableMetadata metadata;
        int retries = 0;
        final int waitTime = 100;
        do {
            metadata = getSession().getCluster().getMetadata().getKeyspace(this.keyspace).getTable(this
                    .columnFamily);

            if (metadata != null) {
                continue;
            }

            LOG.warn(String.format("Metadata for new table %s.%s NOT FOUND, waiting %d millis", this.keyspace, this.columnFamily, waitTime));
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                LOG.error("Sleep interrupted", e);
            }

            retries++;

            if (retries >= 10) {
                throw new DeepIOException("Cannot retrieve metadata for the newly created CF ");
            }
        } while (metadata == null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Map<String, Cell> columnDefinitions() {
        if (columnDefinitionMap != null) {
            return columnDefinitionMap;
        }

        TableMetadata tableMetadata = fetchTableMetadata();

        if (tableMetadata == null && !createTableOnWrite) {
            LOG.warn("Configuration not suitable for writing RDD: output table does not exists and configuration " +
                    "object has 'createTableOnWrite' = false");

            return null;
        } else if (tableMetadata == null) {
            return null;
        }

        initColumnDefinitionMap(tableMetadata);

        return columnDefinitionMap;
    }

    private void initColumnDefinitionMap(TableMetadata tableMetadata) {
        columnDefinitionMap = new HashMap<>();

        List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
        List<ColumnMetadata> clusteringKeys = tableMetadata.getClusteringColumns();
        List<ColumnMetadata> allColumns = tableMetadata.getColumns();

        for (ColumnMetadata key : partitionKeys) {
            Cell metadata = Cell.create(key.getName(), key.getType(), Boolean.TRUE, Boolean.FALSE, key.getTable().getName());
            columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : clusteringKeys) {
            Cell metadata = Cell.create(key.getName(), key.getType(), Boolean.FALSE, Boolean.TRUE, key.getTable().getName());
            columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : allColumns) {
            Cell metadata = Cell.create(key.getName(), key.getType(), Boolean.FALSE, Boolean.FALSE, key.getTable().getName());
            if (!columnDefinitionMap.containsKey(key.getName())) {
                columnDefinitionMap.put(key.getName(), metadata);
            }
        }
        columnDefinitionMap = Collections.unmodifiableMap(columnDefinitionMap);
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#columnFamily(java.lang.String)
     */
    @Override
    public ICassandraDeepJobConfig<T> columnFamily(String columnFamily) {
        this.columnFamily = columnFamily;

        return this;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#columnFamily(java.lang.String)
     */
    @Override
    public ICassandraDeepJobConfig<T> table(String table) {
        return columnFamily(table);
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#getColumnFamily()
    */
    @Override
    public String getColumnFamily() {
        checkInitialized();
        return columnFamily;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#getColumnFamily()
     */
    @Override
    public String getTable() {
        return getColumnFamily();
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#getHost()
     */
    @Override
    public String getHost() {
        checkInitialized();
        return host;
    }

    @Override
    public String[] getInputColumns() {
        checkInitialized();
        return inputColumns == null ? new String[0] : inputColumns.clone();
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#getKeyspace()
     */
    @Override
    public String getKeyspace() {
        checkInitialized();
        return keyspace;
    }

    @Override
    public String getPartitionerClassName() {
        checkInitialized();
        return partitionerClassName;
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#getPassword()
    */
    @Override
    public String getPassword() {
        checkInitialized();
        return password;
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#getRpcPort()
    */
    @Override
    public Integer getRpcPort() {
        checkInitialized();
        return rpcPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getCqlPort() {
        checkInitialized();
        return cqlPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUsername() {
        checkInitialized();
        return username;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> host(String hostname) {
        this.host = hostname;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> initialize() {
        if (isInitialized) {
            return this;
        }

        if (StringUtils.isEmpty(host)) {
            try {
                host = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                LOG.warn("Cannot resolve local host canonical name, using \"localhost\"");
                host = InetAddress.getLoopbackAddress().getCanonicalHostName();
            }
        }


        validate();

        columnDefinitions();
        isInitialized = Boolean.TRUE;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> inputColumns(String... columns) {
        this.inputColumns = columns;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> keyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    @Override
    public ICassandraDeepJobConfig<T> bisectFactor(int bisectFactor) {
        this.bisectFactor = bisectFactor;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> partitioner(String partitionerClassName) {
        this.partitionerClassName = partitionerClassName;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> password(String password) {
        this.password = password;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> rpcPort(Integer port) {
        this.rpcPort = port;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> cqlPort(Integer port) {
        this.cqlPort = port;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> username(String username) {
        this.username = username;

        return this;
    }

    /**
     * Validates if any of the mandatory fields have been configured or not.
     * Throws an {@link IllegalArgumentException} if any of the mandatory
     * properties have not been configured.
     */
    void validate() {
        validateCassandraParams();

        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize cannot be zero");
        }

        if (pageSize > Constants.DEFAULT_MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize cannot exceed " + Constants.DEFAULT_MAX_PAGE_SIZE);
        }

        validateConsistencyLevels();

        TableMetadata tableMetadata = fetchTableMetadata();

        validateTableMetadata(tableMetadata);
        validateAdditionalFilters(tableMetadata);

        if (bisectFactor != Constants.DEFAULT_BISECT_FACTOR && !checkIsPowerOfTwo(bisectFactor)) {
            throw new IllegalArgumentException("Bisect factor should be greater than zero and a power of 2");
        }
    }

    private void validateCassandraParams() {
        if (StringUtils.isEmpty(host)) {
            throw new IllegalArgumentException("host cannot be null");
        }

        if (rpcPort == null) {
            throw new IllegalArgumentException("rpcPort cannot be null");
        }

        if (StringUtils.isEmpty(keyspace)) {
            throw new IllegalArgumentException("keyspace cannot be null");
        }

        if (StringUtils.isEmpty(columnFamily)) {
            throw new IllegalArgumentException("columnFamily cannot be null");
        }
    }

    private void validateTableMetadata(TableMetadata tableMetadata) {

        if (tableMetadata == null && !isWriteConfig) {
            throw new IllegalArgumentException(String.format("Column family {%s.%s} does not exist", keyspace,
                    columnFamily));
        }

        if (tableMetadata == null && !createTableOnWrite) {
            throw new IllegalArgumentException(String.format("Column family {%s.%s} does not exist and " +
                    "createTableOnWrite = false", keyspace, columnFamily));
        }

        if (!ArrayUtils.isEmpty(inputColumns)) {
            for (String column : inputColumns) {
                assert tableMetadata != null;
                ColumnMetadata columnMetadata = tableMetadata.getColumn(column);

                if (columnMetadata == null) {
                    throw new DeepNoSuchFieldException("No column with name " + column + " has been found on table "
                            + this.keyspace + "." + this.columnFamily);
                }
            }
        }

    }

    private void validateAdditionalFilters(TableMetadata tableMetadata) {
        for (Map.Entry<String, Serializable> entry : additionalFilters.entrySet()) {
            /* check if there's an index specified on the provided column */
            ColumnMetadata columnMetadata = tableMetadata.getColumn(entry.getKey());

            if (columnMetadata == null) {
                throw new DeepNoSuchFieldException("No column with name " + entry.getKey() + " has been found on " +
                        "table " + this.keyspace + "." + this.columnFamily);
            }

            if (columnMetadata.getIndex() == null) {
                throw new DeepIndexNotFoundException("No index has been found on column " + columnMetadata.getName()
                        + " on table " + this.keyspace + "." + this.columnFamily);
            }
        }
    }

    private void validateConsistencyLevels() {
        if (readConsistencyLevel != null) {
            try {
                ConsistencyLevel.valueOf(readConsistencyLevel);

            } catch (Exception e) {
                throw new IllegalArgumentException("readConsistencyLevel not valid, " +
                        "should be one of thos defined in org.apache.cassandra.db.ConsistencyLevel", e);
            }
        }

        if (writeConsistencyLevel != null) {
            try {
                ConsistencyLevel.valueOf(writeConsistencyLevel);

            } catch (Exception e) {
                throw new IllegalArgumentException("writeConsistencyLevel not valid, " +
                        "should be one of thos defined in org.apache.cassandra.db.ConsistencyLevel", e);
            }
        }
    }

    private boolean checkIsPowerOfTwo(int n) {
        return (n > 0) && ((n & (n - 1)) == 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean isCreateTableOnWrite() {
        return createTableOnWrite;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> createTableOnWrite(Boolean createTableOnWrite) {
        this.createTableOnWrite = createTableOnWrite;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    public Map<String, Serializable> getAdditionalFilters() {
        return Collections.unmodifiableMap(additionalFilters);
    }

    @Override
    public int getPageSize() {
        checkInitialized();
        return this.pageSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue) {
        /* check if there's an index specified on the provided column */
        additionalFilters.put(filterColumnName, filterValue);
        return this;
    }

    @Override
    public ICassandraDeepJobConfig<T> pageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> readConsistencyLevel(String level) {
        this.readConsistencyLevel = level;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> writeConsistencyLevel(String level) {
        this.writeConsistencyLevel = level;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean getIsWriteConfig() {
        return isWriteConfig;
    }

    @Override
    public int getBisectFactor() {
        return bisectFactor;
    }
}
