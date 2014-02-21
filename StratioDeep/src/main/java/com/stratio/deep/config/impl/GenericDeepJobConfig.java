package com.stratio.deep.config.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.*;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.cql.DeepConfigHelper;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.util.Constants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

/**
 * Created by luca on 04/02/14.
 */
public abstract class GenericDeepJobConfig<T> implements IDeepJobConfig<T> {
    private static Logger logger = Logger.getLogger("com.stratio.deep.config.impl.GenericDeepJobConfig");
    private static final long serialVersionUID = -7179376653643603038L;
    private String partitionerClassName = "org.apache.cassandra.dht.Murmur3Partitioner";

    private transient Job hadoopJob;

    private transient Configuration configuration;

    protected transient ColumnFamilyStore columnFamilyStore;

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
    private String defaultFilter;

    private Map<String, String> additionalFilters = new TreeMap<>();

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

    private transient Map<String, Cell> columnDefinitionMap;

    protected void checkInitialized() {
	if (configuration == null) {
	    throw new DeepIllegalAccessException("EntityDeepJobConfig has not been initialized!");
	}
    }

    @Override
    public Map<String, Cell> columnDefinitions() {
	if (columnDefinitionMap != null) {
	    return columnDefinitionMap;
	}

	logger.info("Connecting to " + getHost() + ":" + getCqlPort());

	Cluster cluster = Cluster.builder().withPort(getCqlPort())
			.addContactPoint(getHost()).build();

	Session session = cluster.connect();

	KeyspaceMetadata ksMetadata = session.getCluster().getMetadata().getKeyspace(getKeyspace());

	TableMetadata tableMetadata = ksMetadata.getTable(getTable());
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

	session.shutdown();

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

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#defaultFilter(java.lang.String)
    */
    @Override
    public IDeepJobConfig<T> defaultFilter(String defaultFilter) {
	this.defaultFilter = defaultFilter;

	return this;
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
     * @see com.stratio.deep.config.IDeepJobConfig#getDefaultFilter()
     */
    @Override
    public String getDefaultFilter() {
	checkInitialized();
	return defaultFilter;
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

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#getRpcPort()
    */
    @Override
    public Integer getCqlPort() {
	checkInitialized();
	return cqlPort;
    }

    @Override
    public Integer getThriftFramedTransportSizeMB() {
	checkInitialized();
	return thriftFramedTransportSizeMB;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#getUsername()
     */
    @Override
    public String getUsername() {
	checkInitialized();
	return username;
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#host(java.lang.String)
    */
    @Override
    public IDeepJobConfig<T> host(String hostname) {
	this.host = hostname;

	return this;
    }

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

	    if (StringUtils.isNotEmpty(defaultFilter)) {
		CqlConfigHelper.setInputWhereClauses(c, defaultFilter);
	    }

	    if (!ArrayUtils.isEmpty(inputColumns)) {
		CqlConfigHelper.setInputColumns(c, StringUtils.join(inputColumns, ","));
	    }

	    DeepConfigHelper.setOutputBatchSize(c, batchSize);

	    ConfigHelper.setOutputInitialAddress(c, host);
	    ConfigHelper.setOutputRpcPort(c, String.valueOf(rpcPort));
	    ConfigHelper.setOutputPartitioner(c, partitionerClassName);

	    ConfigHelper.setThriftFramedTransportSizeInMb(c, thriftFramedTransportSizeMB);

	    configuration = c;

	    columnDefinitions();
	} catch (IOException e) {
	    throw new DeepIOException(e);
	}
	return this;
    }

    /*
     * (non-Jvadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#inputColumns(java.lang.String[])
     */
    @Override
    public IDeepJobConfig<T> inputColumns(String... columns) {
	this.inputColumns = columns;

	return this;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#keyspace(java.lang.String)
     */
    @Override
    public IDeepJobConfig<T> keyspace(String keyspace) {
	this.keyspace = keyspace;
	return this;
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#partitioner(org.apache.cassandra.dht.IPartitioner)
    */
    @Override
    public <P extends IPartitioner<?>> IDeepJobConfig<T> partitioner(String partitionerClassName) {
	this.partitionerClassName = partitionerClassName;
	return this;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#password(java.lang.String)
     */
    @Override
    public IDeepJobConfig<T> password(String password) {
	this.password = password;

	return this;
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#rpcPort(java.lang.Integer)
    */
    @Override
    public IDeepJobConfig<T> rpcPort(Integer port) {
	this.rpcPort = port;

	return this;
    }

    @Override
    public IDeepJobConfig<T> cqlPort(Integer port) {
	this.cqlPort = port;

	return this;
    }

    /* (non-Javadoc)
    * @see com.stratio.deep.config.IDeepJobConfig#username(java.lang.String)
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
    }

    /*
     * (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#batchSize()
     */
    @Override
    public IDeepJobConfig<T> batchSize(int batchSize) {
	this.batchSize = batchSize;
	return this;
    }

    public Map<String, String> getAdditionalFilters() {
	return additionalFilters;
    }

    public IDeepJobConfig<T> removeFilter(String fieldName){
	additionalFilters.remove(fieldName);
	return this;
    }

    public IDeepJobConfig<T> addFilter(String fieldName, String filter){
	additionalFilters.put(fieldName, filter);
	return this;
    }
}
