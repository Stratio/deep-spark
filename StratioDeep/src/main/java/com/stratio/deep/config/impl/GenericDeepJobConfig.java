package com.stratio.deep.config.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.*;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.cql.DeepConfigHelper;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.*;
import com.stratio.deep.util.Constants;
import com.stratio.deep.util.Utils;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * Base class for all config implementations providing default implementations for methods
 * defined in {@link com.stratio.deep.config.IDeepJobConfig}.
 */
public abstract class GenericDeepJobConfig<T> implements IDeepJobConfig<T> {
    private static Logger logger = Logger.getLogger("com.stratio.deep.config.impl.GenericDeepJobConfig");
    private static final long serialVersionUID = -7179376653643603038L;
    private String partitionerClassName = "org.apache.cassandra.dht.Murmur3Partitioner";

    private transient Job hadoopJob;

    private transient Configuration configuration;

    /**
     * keyspace name
     */
    private String keyspace;

    /**
     * name of the columnFamily from which data will be fetched
     */
    protected String columnFamily;

    /**
     * hostname of the cassandra server
     */
    private String host = Constants.DEFAULT_CASSANDRA_HOST;

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
     * size of thrift frame in MBs.
     */
    private Integer thriftFramedTransportSizeMB = 256;

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
    private String readConsistencyLevel;

    /**
     * Default write consistency level. Defaults to LOCAL_ONE.
     */
    private String writeConsistencyLevel;

    /**
     * Enables/Disables auto-creation of column family when writing to Cassandra.
     * By Default we do not create the output column family.
     */
    private Boolean createTableOnWrite = Boolean.FALSE;

    /**
     * Checks if this configuration object has been initialized or not.
     *
     * @throws com.stratio.deep.exception.DeepIllegalAccessException if not initialized
     */
    protected void checkInitialized() {
        if (configuration == null) {
            throw new DeepIllegalAccessException("EntityDeepJobConfig has not been initialized!");
        }
    }

    /**
     * Fetches table metadata from the underlying datastore, using DataStax java driver.
     *
     * @return
     */
    private TableMetadata fetchTableMetadata() {
        Cluster cluster = Cluster.builder().withPort(this.cqlPort)
            .addContactPoint(this.host).build();

        try (Session session = cluster.connect()) {
            Metadata metadata = session.getCluster().getMetadata();
            KeyspaceMetadata ksMetadata = null;

            if (metadata != null && (ksMetadata = metadata.getKeyspace(this.keyspace)) != null){
                return ksMetadata.getTable(this.columnFamily);
            } else
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
     * @param tupleRDD
     */
    public void createOutputTableIfNeeded(RDD<Tuple2<Cells, Cells>> tupleRDD) {
        Cluster cluster = Cluster.builder().withPort(this.cqlPort)
            .addContactPoint(this.host).build();

        try (Session session = cluster.connect()) {
            TableMetadata metadata = session.getCluster().getMetadata().getKeyspace(this.keyspace).getTable(this.columnFamily);

            if (metadata == null && !createTableOnWrite) {
                throw new DeepIOException("Cannot write RDD, output table does not exists and configuration object has 'createTableOnWrite' = false");
            }

            if (metadata != null) {
                return;
            }

            Tuple2<Cells, Cells> first = tupleRDD.first();

            if (first._1() == null || first._1().size() == 0){
                throw new DeepNoSuchFieldException("no key structure found on row metadata");
            }

            String createTableQuery = Utils.createTableQueryGenerator(first._1(), first._2(), getKeyspace(), getColumnFamily());

            session.execute(createTableQuery);
        }
    }

    /**
     * Fetches table metadata from Casandra and generates a Map<K, V> where the key is the column name, and the value
     * is the {@link com.stratio.deep.entity.Cell} containing column's metadata.
     *
     * @return
     */
    @Override
    public Map<String, Cell> columnDefinitions() {
        if (columnDefinitionMap != null) {
            return columnDefinitionMap;
        }

        TableMetadata tableMetadata = fetchTableMetadata();

        if (tableMetadata == null && !createTableOnWrite) {
            logger.warn("Cannot write RDD, output table does not exists and configuration object has 'createTableOnWrite' = false");

            return null;
        } else if (tableMetadata == null) {
            return null;
        }

        columnDefinitionMap = new HashMap<>();

        List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
        List<ColumnMetadata> clusteringKeys = tableMetadata.getClusteringColumns();
        List<ColumnMetadata> allColumns = tableMetadata.getColumns();

        for (ColumnMetadata key : partitionKeys) {
            Cell metadata = Cell.createMetadataCell(key.getName(), key.getType().asJavaClass(), Boolean.TRUE, Boolean.FALSE);
            columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : clusteringKeys) {
            Cell metadata = Cell.createMetadataCell(key.getName(), key.getType().asJavaClass(), Boolean.FALSE, Boolean.TRUE);
            columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : allColumns) {
            Cell metadata = Cell.createMetadataCell(key.getName(), key.getType().asJavaClass(), Boolean.FALSE, Boolean.FALSE);
            if (!columnDefinitionMap.containsKey(key.getName())) {
                columnDefinitionMap.put(key.getName(), metadata);
            }
        }
        return columnDefinitionMap;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#columnFamily(java.lang.String)
     */
    @Override
    public IDeepJobConfig<T> columnFamily(String columnFamily) {
        this.columnFamily = columnFamily;

        return this;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#columnFamily(java.lang.String)
     */
    @Override
    public IDeepJobConfig<T> table(String table) {
        return columnFamily(table);
    }

    @Override
    public IDeepJobConfig<T> framedTransportSize(Integer thriftFramedTransportSizeMB) {
        this.thriftFramedTransportSizeMB = thriftFramedTransportSizeMB;

        return this;
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
     * @see com.stratio.deep.config.IDeepJobConfig#initialize()
     */
    @Override
    public Configuration getConfiguration() {
        if (configuration != null) {
            return configuration;
        }

        initialize();

        return configuration;
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
        return inputColumns.clone();
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
    public Integer getThriftFramedTransportSizeMB() {
        checkInitialized();
        return thriftFramedTransportSizeMB;
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
    public IDeepJobConfig<T> host(String hostname) {
        this.host = hostname;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> initialize() {
        if (configuration != null) {
            return this;
        }

        validate();

        try {
            hadoopJob = new Job();
            Configuration c = hadoopJob.getConfiguration();

            ConfigHelper.setInputColumnFamily(c, keyspace, columnFamily, false);
            ConfigHelper.setOutputColumnFamily(c, keyspace, columnFamily);
            ConfigHelper.setInputInitialAddress(c, host);
            ConfigHelper.setInputRpcPort(c, String.valueOf(rpcPort));
            ConfigHelper.setInputPartitioner(c, partitionerClassName);

            if (!ArrayUtils.isEmpty(inputColumns)) {
                CqlConfigHelper.setInputColumns(c, StringUtils.join(inputColumns, ","));
            }

            DeepConfigHelper.setOutputBatchSize(c, batchSize);

            ConfigHelper.setOutputInitialAddress(c, host);
            ConfigHelper.setOutputRpcPort(c, String.valueOf(rpcPort));
            ConfigHelper.setOutputPartitioner(c, partitionerClassName);

            ConfigHelper.setThriftFramedTransportSizeInMb(c, thriftFramedTransportSizeMB);

            if (readConsistencyLevel != null) {
                ConfigHelper.setReadConsistencyLevel(c, readConsistencyLevel);
            }

            configuration = c;

            columnDefinitions();

        } catch (IOException e) {
            throw new DeepIOException(e);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> inputColumns(String... columns) {
        this.inputColumns = columns;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> keyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <P extends IPartitioner<?>> IDeepJobConfig<T> partitioner(String partitionerClassName) {
        this.partitionerClassName = partitionerClassName;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> password(String password) {
        this.password = password;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> rpcPort(Integer port) {
        this.rpcPort = port;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> cqlPort(Integer port) {
        this.cqlPort = port;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> username(String username) {
        this.username = username;

        return this;
    }

    /**
     * Validates if any of the mandatory fields have been configured or not.
     * Throws an {@link IllegalArgumentException} if any of the mandatory
     * properties have not been configured.
     */
    void validate() {
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

        if (readConsistencyLevel != null) {
            try {
                org.apache.cassandra.db.ConsistencyLevel.valueOf(readConsistencyLevel);

            } catch (Exception e) {
                throw new IllegalArgumentException("readConsistencyLevel not valid, should be one of thos defined in org.apache.cassandra.db.ConsistencyLevel", e);
            }
        }

        if (writeConsistencyLevel != null) {
            try {
                org.apache.cassandra.db.ConsistencyLevel.valueOf(writeConsistencyLevel);

            } catch (Exception e) {
                throw new IllegalArgumentException("writeConsistencyLevel not valid, should be one of thos defined in org.apache.cassandra.db.ConsistencyLevel", e);
            }
        }

        TableMetadata tableMetadata = fetchTableMetadata();

        if (tableMetadata == null){
            return;
        }

        for (Map.Entry<String, Serializable> entry : additionalFilters.entrySet()) {
            /* check if there's an index specified on the provided column */
            ColumnMetadata columnMetadata = tableMetadata.getColumn(entry.getKey());

            if (columnMetadata == null) {
                throw new DeepNoSuchFieldException("No column with name " + entry.getKey() + " has been found on table " + this.keyspace + "." + this.columnFamily);
            }

            if (columnMetadata.getIndex() == null) {
                throw new DeepIndexNotFoundException("No index has been found on column " + columnMetadata.getName() + " on table " + this.keyspace + "." + this.columnFamily);
            }
        }


    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> batchSize(int batchSize) {
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
    public IDeepJobConfig<T> createTableOnWrite(Boolean createTableOnWrite) {
        this.createTableOnWrite = createTableOnWrite;

        return this;
    }

    /**
     * Returns the map of additional filters specified by the user.
     *
     * @return
     */
    public Map<String, Serializable> getAdditionalFilters() {
        return Collections.unmodifiableMap(additionalFilters);
    }

    /**
     * Adds a new filter for the Cassandra underlying datastore.<br/>
     * Once a new filter has been added, all subsequent queries generated to the underlying datastore
     * will include the filter on the specified column called <i>filterColumnName</i>.
     * Before propagating the filter we check if an index exists in Cassandra.
     *
     * @param filterColumnName
     * @param filterValue
     * @return
     * @throws DeepIndexNotFoundException                          if the specified field has not been indexed in Cassandra.
     * @throws com.stratio.deep.exception.DeepNoSuchFieldException if the specified field is not a valid column in Cassandra.
     */
    @Override
    public IDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue) {
        /* check if there's an index specified on the provided column */
        additionalFilters.put(filterColumnName, filterValue);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> readConsistencyLevel(String level) {
        this.readConsistencyLevel = level;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> writeConsistencyLevel(String level) {
        this.writeConsistencyLevel = level;
        return this;
    }
}
