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

import static com.stratio.deep.rdd.CassandraRDDUtils.createTableQueryGenerator;
import static com.stratio.deep.utils.Utils.quote;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.stratio.deep.entity.CassandraCell;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.exception.DeepIndexNotFoundException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.utils.Constants;

/**
 * Base class for all config implementations providing default implementations
 * for methods defined in {@link com.stratio.deep.config.IDeepJobConfig}.
 */
public abstract class GenericDeepJobConfig<T> implements
        ICassandraDeepJobConfig<T>, AutoCloseable {

    private static final Logger LOG = Logger
            .getLogger("com.stratio.deep.config.GenericDeepJobConfig");
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
    private final Map<String, Serializable> additionalFilters = new TreeMap<>();

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
     * Enables/Disables auto-creation of column family when writing to
     * Cassandra. By Default we do not create the output column family.
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
        if (this.session == null) {
            Cluster cluster = Cluster.builder().withPort(this.cqlPort)
                    .addContactPoint(this.host)
                    .withCredentials(this.username, this.password).build();

            this.session = cluster.connect(quote(this.keyspace));
        }

        return this.session;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("closing " + this.getClass().getCanonicalName());
        if (this.session != null) {
            this.session.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @Override protected void finalize() { LOG.debug("finalizing " +
     *           getClass().getCanonicalName()); close(); }
     */
    /**
     * Checks if this configuration object has been initialized or not.
     * 
     * @throws com.stratio.deep.exception.DeepIllegalAccessException
     *             if not initialized
     */
    protected void checkInitialized() {
        if (!this.isInitialized) {
            throw new DeepIllegalAccessException(
                    "DeepJobConfig has not been initialized!");
        }
    }

    /**
     * Fetches table metadata from the underlying datastore, using DataStax java
     * driver.
     * 
     * @return the table metadata as returned by the driver.
     */
    public TableMetadata fetchTableMetadata() {

        Metadata metadata = this.getSession().getCluster().getMetadata();
        KeyspaceMetadata ksMetadata = metadata
                .getKeyspace(quote(this.keyspace));

        if (ksMetadata != null) {
            return ksMetadata.getTable(quote(this.columnFamily));
        } else {
            return null;
        }
    }

    /**
     * Creates the output column family if not exists. <br/>
     * We first check if the column family exists. <br/>
     * If not, we get the first element from <i>tupleRDD</i> and we use it as a
     * template to get columns metadata.
     * <p>
     * This is a very heavy operation since to obtain the schema we need to get
     * at least one element of the output RDD.
     * </p>
     * 
     * @param tupleRDD
     *            the pair RDD.
     */
    public void createOutputTableIfNeeded(RDD<Tuple2<Cells, Cells>> tupleRDD) {

        TableMetadata metadata = this.getSession().getCluster().getMetadata()
                .getKeyspace(this.keyspace).getTable(quote(this.columnFamily));

        if (metadata == null && !this.createTableOnWrite) {
            throw new DeepIOException(
                    "Cannot write RDD, output table does not exists and configuration object has "
                            + "'createTableOnWrite' = false");
        }

        if (metadata != null) {
            return;
        }

        Tuple2<Cells, Cells> first = tupleRDD.first();

        if (first._1() == null || first._1().isEmpty()) {
            throw new DeepNoSuchFieldException(
                    "no key structure found on row metadata");
        }
        String createTableQuery = createTableQueryGenerator(first._1(),
                first._2(), this.keyspace, quote(this.columnFamily));
        this.getSession().execute(createTableQuery);
        this.waitForNewTableMetadata();
    }

    /**
     * waits until table metadata is not null
     */
    private void waitForNewTableMetadata() {
        TableMetadata metadata;
        int retries = 0;
        final int waitTime = 100;
        do {
            metadata = this.getSession().getCluster().getMetadata()
                    .getKeyspace(this.keyspace)
                    .getTable(quote(this.columnFamily));

            if (metadata != null) {
                continue;
            }

            LOG.warn(String
                    .format("Metadata for new table %s.%s NOT FOUND, waiting %d millis",
                            this.keyspace, this.columnFamily, waitTime));
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                LOG.error("Sleep interrupted", e);
            }

            retries++;

            if (retries >= 10) {
                throw new DeepIOException(
                        "Cannot retrieve metadata for the newly created CF ");
            }
        } while (metadata == null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Map<String, Cell> columnDefinitions() {
        if (this.columnDefinitionMap != null) {
            return this.columnDefinitionMap;
        }

        TableMetadata tableMetadata = this.fetchTableMetadata();

        if (tableMetadata == null && !this.createTableOnWrite) {
            LOG.warn("Configuration not suitable for writing RDD: output table does not exists and configuration "
                    + "object has 'createTableOnWrite' = false");

            return null;
        } else if (tableMetadata == null) {
            return null;
        }

        this.initColumnDefinitionMap(tableMetadata);

        return this.columnDefinitionMap;
    }

    private void initColumnDefinitionMap(TableMetadata tableMetadata) {
        this.columnDefinitionMap = new HashMap<>();

        List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
        List<ColumnMetadata> clusteringKeys = tableMetadata
                .getClusteringColumns();
        List<ColumnMetadata> allColumns = tableMetadata.getColumns();

        for (ColumnMetadata key : partitionKeys) {
            Cell metadata = CassandraCell.create(key.getName(), key.getType(),
                    Boolean.TRUE, Boolean.FALSE);
            this.columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : clusteringKeys) {
            Cell metadata = CassandraCell.create(key.getName(), key.getType(),
                    Boolean.FALSE, Boolean.TRUE);
            this.columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : allColumns) {
            Cell metadata = CassandraCell.create(key.getName(), key.getType(),
                    Boolean.FALSE, Boolean.FALSE);
            if (!this.columnDefinitionMap.containsKey(key.getName())) {
                this.columnDefinitionMap.put(key.getName(), metadata);
            }
        }
        this.columnDefinitionMap = Collections
                .unmodifiableMap(this.columnDefinitionMap);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.deep.config.IDeepJobConfig#columnFamily(java.lang.String)
     */
    @Override
    public ICassandraDeepJobConfig<T> columnFamily(String columnFamily) {
        this.columnFamily = columnFamily;

        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.deep.config.IDeepJobConfig#columnFamily(java.lang.String)
     */
    @Override
    public ICassandraDeepJobConfig<T> table(String table) {
        return this.columnFamily(table);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IDeepJobConfig#getColumnFamily()
     */
    @Override
    public String getColumnFamily() {
        this.checkInitialized();
        return this.columnFamily;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IDeepJobConfig#getColumnFamily()
     */
    @Override
    public String getTable() {
        return this.getColumnFamily();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IDeepJobConfig#getHost()
     */
    @Override
    public String getHost() {
        this.checkInitialized();
        return this.host;
    }

    @Override
    public String[] getInputColumns() {
        this.checkInitialized();
        return this.inputColumns == null ? new String[0] : this.inputColumns
                .clone();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IDeepJobConfig#getKeyspace()
     */
    @Override
    public String getKeyspace() {
        this.checkInitialized();
        return this.keyspace;
    }

    @Override
    public String getPartitionerClassName() {
        this.checkInitialized();
        return this.partitionerClassName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IDeepJobConfig#getPassword()
     */
    @Override
    public String getPassword() {
        this.checkInitialized();
        return this.password;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IDeepJobConfig#getRpcPort()
     */
    @Override
    public Integer getRpcPort() {
        this.checkInitialized();
        return this.rpcPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getCqlPort() {
        this.checkInitialized();
        return this.cqlPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUsername() {
        this.checkInitialized();
        return this.username;
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
        if (this.isInitialized) {
            return this;
        }

        if (StringUtils.isEmpty(this.host)) {
            try {
                this.host = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                LOG.warn("Cannot resolve local host canonical name, using \"localhost\"");
                this.host = InetAddress.getLoopbackAddress()
                        .getCanonicalHostName();
            }
        }

        this.validate();

        this.columnDefinitions();
        this.isInitialized = Boolean.TRUE;

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
        this.validateCassandraParams();

        if (this.pageSize <= 0) {
            throw new IllegalArgumentException("pageSize cannot be zero");
        }

        if (this.pageSize > Constants.DEFAULT_MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize cannot exceed "
                    + Constants.DEFAULT_MAX_PAGE_SIZE);
        }

        this.validateConsistencyLevels();

        TableMetadata tableMetadata = this.fetchTableMetadata();

        this.validateTableMetadata(tableMetadata);
        this.validateAdditionalFilters(tableMetadata);
        if (this.bisectFactor != Constants.DEFAULT_BISECT_FACTOR
                && !this.checkIsPowerOfTwo(this.bisectFactor)) {
            throw new IllegalArgumentException(
                    "Bisect factor should be greater than zero and a power of 2");
        }
    }

    private void validateCassandraParams() {
        if (StringUtils.isEmpty(this.host)) {
            throw new IllegalArgumentException("host cannot be null");
        }

        if (this.rpcPort == null) {
            throw new IllegalArgumentException("rpcPort cannot be null");
        }

        if (StringUtils.isEmpty(this.keyspace)) {
            throw new IllegalArgumentException("keyspace cannot be null");
        }

        if (StringUtils.isEmpty(this.columnFamily)) {
            throw new IllegalArgumentException("columnFamily cannot be null");
        }
    }

    private void validateTableMetadata(TableMetadata tableMetadata) {

        if (tableMetadata == null && !this.isWriteConfig) {
            throw new IllegalArgumentException(String.format(
                    "Column family {%s.%s} does not exist", this.keyspace,
                    this.columnFamily));
        }

        if (tableMetadata == null && !this.createTableOnWrite) {
            throw new IllegalArgumentException(String.format(
                    "Column family {%s.%s} does not exist and "
                            + "createTableOnWrite = false", this.keyspace,
                    this.columnFamily));
        }

        if (!ArrayUtils.isEmpty(this.inputColumns)) {
            for (String column : this.inputColumns) {
                assert tableMetadata != null;
                ColumnMetadata columnMetadata = tableMetadata.getColumn(column);

                if (columnMetadata == null) {
                    throw new DeepNoSuchFieldException("No column with name "
                            + column + " has been found on table "
                            + this.keyspace + "." + this.columnFamily);
                }
            }
        }

    }

    private void validateAdditionalFilters(TableMetadata tableMetadata) {
        for (Map.Entry<String, Serializable> entry : this.additionalFilters
                .entrySet()) {
            /* check if there's an index specified on the provided column */
            ColumnMetadata columnMetadata = tableMetadata.getColumn(entry
                    .getKey());

            if (columnMetadata == null) {
                throw new DeepNoSuchFieldException("No column with name "
                        + entry.getKey() + " has been found on " + "table "
                        + this.keyspace + "." + this.columnFamily);
            }

            if (columnMetadata.getIndex() == null) {
                throw new DeepIndexNotFoundException(
                        "No index has been found on column "
                                + columnMetadata.getName() + " on table "
                                + this.keyspace + "." + this.columnFamily);
            }
        }
    }

    private void validateConsistencyLevels() {
        if (this.readConsistencyLevel != null) {
            try {
                ConsistencyLevel.valueOf(this.readConsistencyLevel);

            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "readConsistencyLevel not valid, "
                                + "should be one of thos defined in org.apache.cassandra.db.ConsistencyLevel",
                        e);
            }
        }

        if (this.writeConsistencyLevel != null) {
            try {
                ConsistencyLevel.valueOf(this.writeConsistencyLevel);

            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "writeConsistencyLevel not valid, "
                                + "should be one of those defined in org.apache.cassandra.db.ConsistencyLevel",
                        e);
            }
        }
    }

    private boolean checkIsPowerOfTwo(int n) {
        return n > 0 && (n & n - 1) == 0;
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
        return this.createTableOnWrite;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> createTableOnWrite(
            Boolean createTableOnWrite) {
        this.createTableOnWrite = createTableOnWrite;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Serializable> getAdditionalFilters() {
        return Collections.unmodifiableMap(this.additionalFilters);
    }

    @Override
    public int getPageSize() {
        this.checkInitialized();
        return this.pageSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ICassandraDeepJobConfig<T> filterByField(String filterColumnName,
            Serializable filterValue) {
        /* check if there's an index specified on the provided column */
        this.additionalFilters.put(filterColumnName, filterValue);
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
        return this.readConsistencyLevel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteConsistencyLevel() {
        return this.writeConsistencyLevel;
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
        return this.batchSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean getIsWriteConfig() {
        return this.isWriteConfig;
    }

    @Override
    public int getBisectFactor() {
        return this.bisectFactor;
    }

}
