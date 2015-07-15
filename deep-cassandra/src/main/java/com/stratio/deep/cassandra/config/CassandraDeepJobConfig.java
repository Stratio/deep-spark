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

package com.stratio.deep.cassandra.config;

import static com.stratio.deep.cassandra.util.CassandraUtils.createTableQueryGenerator;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.BATCHSIZE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.BISECT_FACTOR;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.CQLPORT;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.CREATE_ON_WRITE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.PAGE_SIZE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.READ_CONSISTENCY_LEVEL;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.RPCPORT;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.WRITE_CONSISTENCY_LEVEL;
import static com.stratio.deep.commons.utils.Utils.quote;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.stratio.deep.cassandra.util.InetAddressConverter;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.stratio.deep.cassandra.filter.value.EqualsInValue;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.exception.DeepIndexNotFoundException;
import com.stratio.deep.commons.exception.DeepNoSuchFieldException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.utils.Constants;

import scala.Tuple2;

/**
 * Base class for all config implementations providing default implementations for methods defined in
 * {@link ICassandraDeepJobConfig}.
 */
public abstract class CassandraDeepJobConfig<T> extends DeepJobConfig<T, CassandraDeepJobConfig<T>> implements
        AutoCloseable,
        ICassandraDeepJobConfig<T> {

    private static final Logger LOG = Logger.getLogger("com.stratio.deep.config.GenericICassandraDeepJobConfig");

    private static final long serialVersionUID = -7179376653643603038L;

    private String partitionerClassName = "org.apache.cassandra.dht.Murmur3Partitioner";

    /**
     * Cassandra server RPC port.
     */
    private Integer rpcPort = Constants.DEFAULT_CASSANDRA_RPC_PORT;

    /**
     * Cassandra server CQL port.
     */
    private Integer cqlPort = Constants.DEFAULT_CASSANDRA_CQL_PORT;

    /**
     * default "where" filter to use to access ColumnFamily's data.
     */
    private final Map<String, Serializable> additionalFilters = new TreeMap<>();


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
     * Enables/Disables auto-creation of column family when writing to Cassandra. By Default we do not create the output
     * column family.
     */
    protected Boolean createTableOnWrite = Boolean.TRUE;

    private transient Session session;

    private Boolean isInitialized = Boolean.FALSE;

    private int pageSize = Constants.DEFAULT_PAGE_SIZE;

    protected Boolean isWriteConfig = Boolean.TRUE;

    private int bisectFactor = Constants.DEFAULT_BISECT_FACTOR;

    private final int splitSize = Constants.DEFAULT_SPLIT_SIZE;

    private boolean isSplitModeSet = false;

    private boolean isBisectModeSet = true;

    private EqualsInValue equalsInValue = null;

    /**
     * {@inheritDoc}
     */
    @Override
    public CassandraDeepJobConfig<T> session(Session session) {
        this.session = session;
        return this;
    }

    /**
     * {@inheritDoc}
     */


    private static Map<String, Session > cassandraSession = new HashedMap();
    @Override
    public synchronized Session getSession() {
        String id = this.getHost()+":"+this.cqlPort;
        if (!cassandraSession.containsKey(id)){
            Cluster cluster = Cluster.builder()
                    .withPort(this.cqlPort)
                    .addContactPoints(InetAddressConverter.toInetAddress(this.getHost()))
                    .withCredentials(this.username, this.password)
                    .withProtocolVersion(PROTOCOL_VERSION)
                    .build();

            session = cluster.connect(quote(this.catalog));
            cassandraSession.put(id,session);
        }

        return cassandraSession.get(id);
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

    public CassandraDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
    }

    /**
     * {@inheritDoc}
     *
     * @Override protected void finalize() { LOG.debug("finalizing " + getClass().getCanonicalName()); close(); }
     */
    /**
     * Checks if this configuration object has been initialized or not.
     *
     * @throws com.stratio.deep.commons.exception.DeepIllegalAccessException if not initialized
     */
    protected void checkInitialized() {
        if (!isInitialized) {
            initialize();
            LOG.warn("CassandraDeepJobConfig has not been initialized!");
        }
    }

    /**
     * Fetches table metadata from the underlying datastore, using DataStax java driver.
     *
     * @return the table metadata as returned by the driver.
     */
    public TableMetadata fetchTableMetadata() {

        Metadata metadata = getSession().getCluster().getMetadata();
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(quote(this.catalog));

        if (ksMetadata != null) {
            return ksMetadata.getTable(quote(this.table));
        } else {
            return null;
        }
    }

    /**
     * Creates the output column family if not exists. <br/>
     * We first check if the column family exists. <br/>
     * If not, we get the first element from <i>tupleRDD</i> and we use it as a template to get columns metadata.
     * <p>
     * This is a very heavy operation since to obtain the schema we need to get at least one element of the output RDD.
     * </p>
     *
     * @param first the pair RDD.
     */
    public void createOutputTableIfNeeded(Tuple2<Cells, Cells> first) {

        TableMetadata metadata = getSession()
                .getCluster()
                .getMetadata()
                .getKeyspace(this.catalog)
                .getTable(quote(this.table));

        if (metadata == null && !createTableOnWrite) {
            throw new DeepIOException("Cannot write RDD, output table does not exists and configuration object has " +
                    "'createTableOnWrite' = false");
        }

        if (metadata != null) {
            return;
        }

        if (first._1() == null || first._1().isEmpty()) {
            throw new DeepNoSuchFieldException("no key structure found on row metadata");
        }
        String createTableQuery = createTableQueryGenerator(first._1(), first._2(), this.catalog,
                quote(this.table));
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
            metadata = getSession()
                    .getCluster()
                    .getMetadata()
                    .getKeyspace(this.catalog)
                    .getTable(quote(this.table));

            if (metadata != null) {
                continue;
            }

            LOG.warn(String.format("Metadata for new table %s.%s NOT FOUND, waiting %d millis", this.catalog,
                    this.table, waitTime));
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
            Cell metadata = Cell.create(key.getName(), key.getType(), Boolean.TRUE, Boolean.FALSE);
            columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : clusteringKeys) {
            Cell metadata = Cell.create(key.getName(), key.getType(), Boolean.FALSE, Boolean.TRUE);
            columnDefinitionMap.put(key.getName(), metadata);
        }

        for (ColumnMetadata key : allColumns) {
            Cell metadata = Cell.create(key.getName(), key.getType(), Boolean.FALSE, Boolean.FALSE);
            if (!columnDefinitionMap.containsKey(key.getName())) {
                columnDefinitionMap.put(key.getName(), metadata);
            }
        }
        columnDefinitionMap = Collections.unmodifiableMap(columnDefinitionMap);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#columnFamily(java.lang.String)
     */

    @Override
    public CassandraDeepJobConfig<T> columnFamily(String columnFamily) {
        this.table = columnFamily;

        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#columnFamily(java.lang.String)
     */

    @Override
    public CassandraDeepJobConfig<T> table(String table) {
        return columnFamily(table);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#getColumnFamily()
     */

    @Override
    public String getColumnFamily() {
        checkInitialized();
        return table;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#getColumnFamily()
     */

    @Override
    public String getTable() {
        return getColumnFamily();
    }



    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#getKeyspace()
     */

    @Override
    public String getKeyspace() {
        checkInitialized();
        return catalog;
    }

    @Override
    public String getPartitionerClassName() {
        checkInitialized();
        return partitionerClassName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#getPassword()
     */

    @Override
    public String getPassword() {
        checkInitialized();
        return password;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.config.IICassandraDeepJobConfig#getRpcPort()
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
    public CassandraDeepJobConfig<T> initialize() {
        if (isInitialized) {
            return this;
        }

        if (StringUtils.isEmpty(getHost())) {
            try {
                host.add(InetAddress.getLocalHost().getCanonicalHostName());
            } catch (UnknownHostException e) {
                LOG.warn("Cannot resolve local host canonical name, using \"localhost\"");
                host.add(InetAddress.getLoopbackAddress().getCanonicalHostName());
            }
        }

        validate();

        columnDefinitions();
        isInitialized = Boolean.TRUE;

        return this;
    }

    @Override
    public CassandraDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {
        super.initialize(extractorConfig);

        Map<String, Serializable> values = extractorConfig.getValues();

        if (values.get(BATCHSIZE) != null) {
            batchSize(extractorConfig.getInteger(BATCHSIZE));
        }

        if (values.get(CQLPORT) != null) {
            cqlPort(extractorConfig.getInteger(CQLPORT));
        }

        if (values.get(RPCPORT) != null) {
            rpcPort(extractorConfig.getInteger(RPCPORT));
        }

        if (values.get(CREATE_ON_WRITE) != null) {
            createTableOnWrite(extractorConfig.getBoolean(CREATE_ON_WRITE));
        }

        if (values.get(PAGE_SIZE) != null) {
            pageSize(extractorConfig.getInteger(PAGE_SIZE));
        }

        if (values.get(READ_CONSISTENCY_LEVEL) != null) {
            readConsistencyLevel(extractorConfig.getString(READ_CONSISTENCY_LEVEL));
        }

        if (values.get(WRITE_CONSISTENCY_LEVEL) != null) {
            writeConsistencyLevel(extractorConfig.getString(WRITE_CONSISTENCY_LEVEL));
        }

        if (values.get(BISECT_FACTOR) != null) {
            bisectFactor(extractorConfig.getInteger(BISECT_FACTOR));
        }

        //        if (values.get(ExtractorConstants.FILTER_FIELD) != null) {
        //            Pair<String, Serializable> filterFields = extractorConfig.getPair(ExtractorConstants.FILTER_FIELD,
        //                    String.class, Serializable.class);
        //            filterByField(filterFields.left, filterFields.right);
        //        }

        if (values.get(ExtractorConstants.FILTER_QUERY) != null) {
            filters(extractorConfig.getFilterArray(ExtractorConstants.FILTER_QUERY));

        }

        if (values.get(ExtractorConstants.EQUALS_IN_FILTER) != null) {
            setEqualsInValue((EqualsInValue) extractorConfig.getValue(EqualsInValue.class,
                    ExtractorConstants.EQUALS_IN_FILTER));
        }

        this.initialize();

        return this;
    }

    @Override
    public CassandraDeepJobConfig<T> filters(Filter... filters) {
        this.filters = filters;

        return this;
    }

    @Override
    public Filter[] getFilters() {
        return filters;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> keyspace(String keyspace) {
        this.catalog = keyspace;
        return this;
    }

    @Override
    public CassandraDeepJobConfig<T> bisectFactor(int bisectFactor) {
        this.isSplitModeSet = false;
        this.isBisectModeSet = true;
        this.bisectFactor = bisectFactor;
        return this;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> partitioner(String partitionerClassName) {
        this.partitionerClassName = partitionerClassName;
        return this;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> password(String password) {
        this.password = password;

        return this;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> rpcPort(Integer port) {
        this.rpcPort = port;

        return this;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> cqlPort(Integer port) {
        this.cqlPort = port;

        return this;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> username(String username) {
        this.username = username;

        return this;
    }

    /**
     * Validates if any of the mandatory fields have been configured or not. Throws an {@link IllegalArgumentException}
     * if any of the mandatory properties have not been configured.
     */
    void validate() {
        validateCassandraParams();

        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize cannot be zero");
        }

        

        validateConsistencyLevels();

        TableMetadata tableMetadata = fetchTableMetadata();

        validateTableMetadata(tableMetadata);
        validateAdditionalFilters(tableMetadata);

        if (!(this.isBisectModeSet && this.isSplitModeSet)) {
            if (this.isBisectModeSet) {
                if (this.bisectFactor != Constants.DEFAULT_BISECT_FACTOR
                        && !this.checkIsPowerOfTwo(this.bisectFactor)) {
                    throw new IllegalArgumentException(
                            "Bisect factor should be greater than zero and a power of 2");
                }
            } else if (this.isSplitModeSet) {
                if (this.splitSize <= 0) {
                    throw new IllegalArgumentException(
                            "The split size must be a positve integer");
                }
            } else {
                throw new IllegalArgumentException(
                        "One split mode must be defined, please choose between Split or Bisect");
            }
        } else {
            throw new IllegalArgumentException(
                    "Only one split mode can be defined, please choose between Split or Bisect");
        }
    }

    private void validateCassandraParams() {
        if (StringUtils.isEmpty(getHost())) {
            throw new IllegalArgumentException("host cannot be null");
        }

        if (rpcPort == null) {
            throw new IllegalArgumentException("rpcPort cannot be null");
        }

        if (StringUtils.isEmpty(catalog)) {
            throw new IllegalArgumentException("keyspace cannot be null");
        }

        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("columnFamily cannot be null");
        }
    }

    private void validateTableMetadata(TableMetadata tableMetadata) {

        if (tableMetadata == null && !isWriteConfig) {
            throw new IllegalArgumentException(String.format("Column family {%s.%s} does not exist", catalog,
                    table));
        }

        if (tableMetadata == null && !createTableOnWrite) {
            throw new IllegalArgumentException(String.format("Column family {%s.%s} does not exist and " +
                    "createTableOnWrite = false", catalog, table));
        }

        if (!ArrayUtils.isEmpty(inputColumns)) {
            for (String column : inputColumns) {
                assert tableMetadata != null;
                ColumnMetadata columnMetadata = tableMetadata.getColumn(column);

                if (columnMetadata == null) {
                    throw new DeepNoSuchFieldException("No column with name " + column + " has been found on table "
                            + this.catalog + "." + this.table);
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
                        "table " + this.catalog + "." + this.table);
            }

            if (columnMetadata.getIndex() == null) {
                throw new DeepIndexNotFoundException("No index has been found on column " + columnMetadata.getName()
                        + " on table " + this.catalog + "." + this.table);
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
                        "should be one of those defined in org.apache.cassandra.db.ConsistencyLevel", e);
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
    public CassandraDeepJobConfig<T> batchSize(int batchSize) {
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
    public CassandraDeepJobConfig<T> createTableOnWrite(Boolean createTableOnWrite) {
        this.createTableOnWrite = createTableOnWrite;
        this.isWriteConfig = createTableOnWrite;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Serializable> getAdditionalFilters() {
        return Collections.unmodifiableMap(additionalFilters);
    }

    public int getPageSize() {
        checkInitialized();
        return this.pageSize;
    }

    public CassandraDeepJobConfig<T> pageSize(int pageSize) {
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
    public CassandraDeepJobConfig<T> readConsistencyLevel(String level) {
        this.readConsistencyLevel = level;

        return this;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public CassandraDeepJobConfig<T> writeConsistencyLevel(String level) {
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

    // TODO: It will be added in a future release
    @Override
    public CassandraDeepJobConfig<T> splitSize(int splitSize) {
        // this.isSplitModeSet = true;
        // this.isBisectModeSet = false;
        // this.splitSize = splitSize;
        return this;
    }

    @Override
    public Integer getSplitSize() {
        return this.splitSize;
    }

    @Override
    public boolean isSplitModeSet() {
        return this.isSplitModeSet;
    }

    @Override
    public boolean isBisectModeSet() {
        return this.isBisectModeSet;
    }

    public EqualsInValue getEqualsInValue() {
        return equalsInValue;
    }

    public void setEqualsInValue(EqualsInValue equalsInValue) {
        this.equalsInValue = equalsInValue;
    }

    public static ProtocolVersion PROTOCOL_VERSION  = ProtocolVersion.V2;

}
