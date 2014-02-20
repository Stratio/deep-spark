package com.stratio.deep.config;

import java.io.Serializable;
import java.util.Map;

import com.stratio.deep.entity.Cell;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.hadoop.conf.Configuration;

/**
 * Defines the public methods that each Stratio Deep configuration object should implement.
 *
 * @param <T>
 */
public interface IDeepJobConfig<T> extends Serializable {

    Map<String, Cell> columnDefinitions();

    /**
     * Sets the cassandra CF from which data will be read from.
     *
     * @param table
     * @return
     */
    public abstract IDeepJobConfig<T> table(String table);

    /**
     * Sets the cassandra CF from which data will be read from.
     *
     * @param columnFamily
     * @return
     */
    public abstract IDeepJobConfig<T> columnFamily(String columnFamily);

    /**
     * Sets the default "where" filter to use to access ColumnFamily's data.
     *
     * @param defaultFilter
     * @return
     */
    public abstract IDeepJobConfig<T> defaultFilter(String defaultFilter);

    /**
     * Sets the default thrift frame size.
     *
     * @param thriftFramedTransportSizeMB
     * @return
     */
    public abstract IDeepJobConfig<T> framedTransportSize(Integer thriftFramedTransportSizeMB);

    /* Getters */

    /**
     * Returns the name of the configured column family.
     * 
     * @return
     */
    public abstract String getColumnFamily();

    /**
     * Validates and initializes this configuration object.
     */
    public abstract Configuration getConfiguration();

    /**
     * Returns the default filter string.
     * 
     * @return
     */
    public abstract String getDefaultFilter();

    /**
     * Returns the underlying entity class used to map the Cassandra
     * Column family.
     * 
     * @return
     */
    public abstract Class<T> getEntityClass();

    /**
     * Returns the hostname of the cassandra server.
     * 
     * @return
     */
    public abstract String getHost();

    /**
     * Returns the list of column names that will 
     * be fetched from the underlying datastore.
     * @return
     */
    public abstract String[] getInputColumns();

    /**
     * Returns the name of the keyspace.
     *  
     * @return
     */
    public abstract String getKeyspace();

    /**
     * Returns the partitioner class name.
     * 
     * @return
     */
    public abstract String getPartitionerClassName();

    /**
     * Returns the password needed to authenticate 
     * to the remote cassandra cluster.
     * @return
     */
    public abstract String getPassword();

    /**
     * RPC port where the remote Cassandra cluster is listening to.
     * Defaults to 9160.
     *
     * @return
     */
    public abstract Integer getRpcPort();

    /**
     * CQL port where the remote Cassandra cluster is listening to.
     * Defaults to 9042.
     *
     * @return
     */
    public abstract Integer getCqlPort();

    /**
     * Returns the thrift frame size.
     * @return
     */
    public abstract Integer getThriftFramedTransportSizeMB();

    /**
     * Returns the username used to authenticate to the cassandra server.
     * Defaults to the empty string.
     * 
     * @return
     */
    public abstract String getUsername();

    /**
     * Sets the cassandra's hostname
     *
     * @param hostname
     * @return this object.
     */
    public abstract IDeepJobConfig<T> host(String hostname);

    /**
     * Initialized the current configuration object.
     *
     * @return this object.
     */
    public abstract IDeepJobConfig<T> initialize();

    /**
     * Defines a projection over the CF columns.
     *
     * @param columns
     * @return this object.
     */
    public abstract IDeepJobConfig<T> inputColumns(String... columns);

    /**
     * Sets Cassandra Keyspace.
     *
     * @param keyspace
     * @return this object.
     */
    public abstract IDeepJobConfig<T> keyspace(String keyspace);

    /**
     * Let's the user specify an alternative partitioner class. The default partitioner is
     * org.apache.cassandra.dht.Murmur3Partitioner.
     *
     * @return this object.
     * @param partitionerClassName
     */
    public abstract <P extends IPartitioner<?>> IDeepJobConfig<T> partitioner(String partitionerClassName);

    /**
     * Sets the password to use to login to Cassandra. Leave empty if you do not need authentication.
     *
     * @return this object.
     */
    public abstract IDeepJobConfig<T> password(String password);

    /**
     * Sets cassandra host rpcPort.
     *
     * @param port
     * @return this object.
     */
    public abstract IDeepJobConfig<T> rpcPort(Integer port);

    /**
     * Sets cassandra host rpcPort.
     *
     * @param port
     * @return this object.
     */
    public abstract IDeepJobConfig<T> cqlPort(Integer port);

    /**
     * Sets the username to use to login to Cassandra. Leave empty if you do not need authentication.
     *
     * @return this object.
     */
    public abstract IDeepJobConfig<T> username(String username);

    /**
     * Sets the batch size used to write to Cassandra.
     * 
     * @return this object.
     */
    public abstract IDeepJobConfig<T> batchSize(int batchSize);

    /**
     * Returns the name of the configured column family.
     * 
     * @return
     */
    public abstract String getTable();

}