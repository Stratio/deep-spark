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

import com.datastax.driver.core.Session;
import com.stratio.deep.entity.Cell;
import org.apache.cassandra.dht.IPartitioner;

import java.io.Serializable;
import java.util.Map;

/**
 * Defines the public methods that each Stratio Deep configuration object should implement.
 *
 * @param <T> the generic type associated to this configuration object.
 */
public interface IDeepJobConfig<T> extends Serializable {

    /**
     * Returns the session opened to the cassandra server.
     *
     * @return the Session opened by this configuration object to the cassandra server.
     */
    Session getSession();

    /**
     * Sets the session to use. If a session is not provided, this object will open a new one.
     *
     * @param session the session to use.
     */
    IDeepJobConfig<T> session(Session session);

    /**
     * Fetches table metadata from Casandra and generates a Map<K, V> where the key is the column name, and the value
     * is the {@link com.stratio.deep.entity.Cell} containing column's metadata.
     *
     * @return the map of column names and the corresponding Cell object containing its metadata.
     */
    Map<String, Cell> columnDefinitions();

    /**
     * Sets the cassandra CF from which data will be read from.
     *
     * @param table the table name.
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> table(String table);

    /**
     * Sets the cassandra CF from which data will be read from.
     * Column family name is case sensitive.
     *
     * @param columnFamily the table name data will be fetched from.
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> columnFamily(String columnFamily);

    /**
     * Adds a new filter for the Cassandra underlying datastore.<br/>
     * Once a new filter has been added, all subsequent queries generated to the underlying datastore
     * will include the filter on the specified column called <i>filterColumnName</i>.
     * Before propagating the filter we check if an index exists in Cassandra.
     *
     * @param filterColumnName the name of the columns (as known by the datastore) to filter on.
     * @param filterValue the value of the filter to use. May be any expression, depends on the actual index implementation.
     * @return this configuration object.
     * @throws com.stratio.deep.exception.DeepIndexNotFoundException if the specified field has not been indexed in
     * Cassandra.
     * @throws com.stratio.deep.exception.DeepNoSuchFieldException   if the specified field is not a valid column in
     * Cassandra.
     */
    public abstract IDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue);

    /**
     * Sets the number of rows to retrieve for each page of data fetched from Cassandra.<br/>
     * Defaults to 1000 rows.
     *
     * @param pageSize the number of rows per page
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> pageSize(int pageSize);


    /* Getters */

    /**
     * Returns the name of the configured column family.
     *
     * @return the configured column family.
     */
    public abstract String getColumnFamily();

    /**
     * Returns the underlying testentity class used to map the Cassandra
     * Column family.
     *
     * @return the entity class object associated to this configuration object.
     */
    public abstract Class<T> getEntityClass();

    /**
     * Returns the hostname of the cassandra server.
     *
     * @return the endpoint of the cassandra server.
     */
    public abstract String getHost();

    /**
     * Returns the list of column names that will
     * be fetched from the underlying datastore.
     *
     * @return the array of column names that will be retrieved from the data store.
     */
    public abstract String[] getInputColumns();

    /**
     * Returns the name of the keyspace.
     *
     * @return the name of the configured keyspace.
     */
    public abstract String getKeyspace();

    /**
     * Returns the partitioner class name.
     *
     * @return the partitioner class name.
     */
    public abstract String getPartitionerClassName();

    /**
     * Returns the password needed to authenticate
     * to the remote cassandra cluster.
     *
     * @return the password used to login to the remote cluster.
     */
    public abstract String getPassword();

    /**
     * RPC port where the remote Cassandra cluster is listening to.
     * Defaults to 9160.
     *
     * @return the thrift port.
     */
    public abstract Integer getRpcPort();

    /**
     * CQL port where the remote Cassandra cluster is listening to.
     * Defaults to 9042.
     *
     * @return the cql port.
     */
    public abstract Integer getCqlPort();

    /**
     * Returns the username used to authenticate to the cassandra server.
     * Defaults to the empty string.
     *
     * @return the username to use to login to the remote server.
     */
    public abstract String getUsername();

    /**
     * Sets the cassandra's hostname
     *
     * @param hostname the cassandra server endpoint.
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
     * Defines a projection over the CF columns. <br/>
     * Key columns will always be returned, even if not specified in the columns input array.
     *
     * @param columns list of columns we want to retrieve from the datastore.
     * @return this object.
     */
    public abstract IDeepJobConfig<T> inputColumns(String... columns);

    /**
     * Sets Cassandra Keyspace.
     *
     * @param keyspace the keyspace to use.
     * @return this object.
     */
    public abstract IDeepJobConfig<T> keyspace(String keyspace);

    /**
     * Sets the token range bisect factor.
     * The provided number must be a power of two.
     * Defaults to 1.
     *
     * @param bisectFactor the bisect factor to use.
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> bisectFactor(int bisectFactor);

    /**
     * Let's the user specify an alternative partitioner class. The default partitioner is
     * org.apache.cassandra.dht.Murmur3Partitioner.
     *
     * @param partitionerClassName the partitioner class name.
     * @return this object.
     */
    public abstract IDeepJobConfig<T> partitioner(String partitionerClassName);

    /**
     * Sets the password to use to login to Cassandra. Leave empty if you do not need authentication.
     *
     * @return this object.
     */
    public abstract IDeepJobConfig<T> password(String password);

    /**
     * Sets cassandra host rpcPort.
     *
     * @param port the thrift port number.
     * @return this object.
     */
    public abstract IDeepJobConfig<T> rpcPort(Integer port);

    /**
     * Sets cassandra host rpcPort.
     *
     * @param port the cql port number.
     * @return this object.
     */
    public abstract IDeepJobConfig<T> cqlPort(Integer port);

    /**
     * /**
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
     * Whether or not to create the output column family on write.<br/>.
     * <p/>
     * Defaults to FALSE.
     *
     * @param createTableOnWrite a boolean that tells this configuration obj to create missing tables on write.
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> createTableOnWrite(Boolean createTableOnWrite);

    /**
     * Returns whether or not in this configuration object we specify to automatically create
     * the output column family.
     *
     * @return true if this configuration object has been configured to create missing tables on writes.
     */
    public abstract Boolean isCreateTableOnWrite();

    /**
     * Returns the configured read consistency level.
     *
     * @return the read consistency level.
     */
    public abstract String getReadConsistencyLevel();

    /**
     * Returns the configured write consistency level.
     *
     * @return the write consistency level.
     */
    public abstract String getWriteConsistencyLevel();

    /**
     * Sets read consistency level. <br/>
     * Can be one of those consistency levels defined in {@link org.apache.cassandra.db.ConsistencyLevel}.<br/>
     * Defaults to {@link org.apache.cassandra.db.ConsistencyLevel#LOCAL_ONE}.
     *
     * @param level the read consistency level to use.
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> readConsistencyLevel(String level);

    /**
     * Sets write consistency level. <br/>
     * Can be one of those consistency levels defined in {@link org.apache.cassandra.db.ConsistencyLevel}.<br/>
     * Defaults to {@link org.apache.cassandra.db.ConsistencyLevel#LOCAL_ONE}.
     *
     * @param level the write consistency level to use.
     * @return this configuration object.
     */
    public abstract IDeepJobConfig<T> writeConsistencyLevel(String level);

    /**
     * Returns the name of the configured column family.
     * Column family name is case sensitive.
     *
     * @return the table name.
     */
    public abstract String getTable();

    /**
     * Returns the batch size used for writing objects to the underying Cassandra datastore.
     *
     * @return the batch size.
     */
    int getBatchSize();

    /**
     * Returns the map of additional filters specified by the user.
     *
     * @return the map of configured additional filters.
     */
    public Map<String, Serializable> getAdditionalFilters();

    /**
     * Returns the maximum number of rows that will be retrieved when fetching data pages from Cassandra.
     *
     * @return the page size
     */
    public int getPageSize();

    /**
     * Returns whether this configuration config is suitable for writing out data to the datastore.
     *
     * @return true if this configuratuon object is suitable for writing to cassandra, false otherwise.
     */
    public Boolean getIsWriteConfig();

    /**
     * @return the configured bisect factor.
     */
    public int getBisectFactor();
}
