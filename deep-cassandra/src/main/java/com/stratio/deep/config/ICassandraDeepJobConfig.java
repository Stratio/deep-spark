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

import java.io.Serializable;
import java.util.Map;

import com.datastax.driver.core.Session;

/**
 * Defines the public methods that each Stratio Deep Cassandra configuration object should implement.
 */
public interface ICassandraDeepJobConfig<T> extends IDeepJobConfig<T, ICassandraDeepJobConfig<T>> {

    /**
     * Adds a new filter for the Cassandra underlying datastore.<br/>
     * Once a new filter has been added, all subsequent queries generated to the underlying datastore
     * will include the filter on the specified column called <i>filterColumnName</i>.
     * Before propagating the filter we check if an index exists in Cassandra.
     *
     * @param filterColumnName the name of the columns (as known by the datastore) to filter on.
     * @param filterValue      the value of the filter to use. May be any expression,
     *                         depends on the actual index implementation.
     * @return this configuration object.
     * @throws com.stratio.deep.exception.DeepIndexNotFoundException if the specified field has not been indexed in
     *                                                               Cassandra.
     * @throws com.stratio.deep.exception.DeepNoSuchFieldException   if the specified field is not a valid column in
     *                                                               Cassandra.
     */
    public ICassandraDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue);

    /**
     * Returns the map of additional filters specified by the user.
     *
     * @return the map of configured additional filters.
     */
    public abstract Map<String, Serializable> getAdditionalFilters();


    /**
     * Returns the partitioner class name.
     *
     * @return the partitioner class name.
     */
    public abstract String getPartitionerClassName();

    /**
     * Let's the user specify an alternative partitioner class. The default partitioner is
     * org.apache.cassandra.dht.Murmur3Partitioner.
     *
     * @param partitionerClassName the partitioner class name.
     * @return this object.
     */
    public abstract ICassandraDeepJobConfig<T> partitioner(String partitionerClassName);

    /**
     * Returns the session opened to the cassandra server.
     *
     * @return the Session opened by this configuration object to the cassandra server.
     */
    public abstract Session getSession();

    /**
     * Returns the name of the keyspace.
     *
     * @return the name of the configured keyspace.
     */
    public abstract String getKeyspace();

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
     * Sets Cassandra Keyspace.
     *
     * @param keyspace the keyspace to use.
     * @return this object.
     */
    public abstract ICassandraDeepJobConfig<T> keyspace(String keyspace);

    /**
     * Sets cassandra host rpcPort.
     *
     * @param port the thrift port number.
     * @return this object.
     */
    public abstract ICassandraDeepJobConfig<T> rpcPort(Integer port);

    /**
     * Sets cassandra host rpcPort.
     *
     * @param port the cql port number.
     * @return this object.
     */
    public abstract ICassandraDeepJobConfig<T> cqlPort(Integer port);

    /**
     * Sets read consistency level. <br/>
     * Can be one of those consistency levels defined in {@link org.apache.cassandra.db.ConsistencyLevel}.<br/>
     * Defaults to {@link org.apache.cassandra.db.ConsistencyLevel#LOCAL_ONE}.
     *
     * @param level the read consistency level to use.
     * @return this configuration object.
     */
    public abstract ICassandraDeepJobConfig<T> readConsistencyLevel(String level);

    /**
     * Sets write consistency level. <br/>
     * Can be one of those consistency levels defined in {@link org.apache.cassandra.db.ConsistencyLevel}.<br/>
     * Defaults to {@link org.apache.cassandra.db.ConsistencyLevel#LOCAL_ONE}.
     *
     * @param level the write consistency level to use.
     * @return this configuration object.
     */
    public abstract ICassandraDeepJobConfig<T> writeConsistencyLevel(String level);

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
     * Sets the token range bisect factor.
     * The provided number must be a power of two.
     * Defaults to 1.
     *
     * @param bisectFactor the bisect factor to use.
     * @return this configuration object.
     */
    public abstract ICassandraDeepJobConfig<T> bisectFactor(int bisectFactor);

    /**
     * @return the configured bisect factor.
     */
    public int getBisectFactor();

    /**
     * Sets the underlying datastore table or collection from which data will be read from.
     *
     * @param table the table name.
     * @return this configuration object.
     */
    public abstract ICassandraDeepJobConfig<T> table(String table);

    /**
     * Returns the name of the configured column family.
     * Column family name is case sensitive.
     *
     * @return the table name.
     */
    public abstract String getTable();

    /**
     * Sets the cassandra CF from which data will be read from.
     * Column family name is case sensitive.
     *
     * @param columnFamily the table name data will be fetched from.
     * @return this configuration object.
     */
    public abstract ICassandraDeepJobConfig<T> columnFamily(String columnFamily);

    /**
     * Returns whether this configuration config is suitable for writing out data to the datastore.
     *
     * @return true if this configuratuon object is suitable for writing to cassandra, false otherwise.
     */
    public abstract Boolean getIsWriteConfig();

    /**
     * Sets the batch size used to write to Cassandra.
     *
     * @return this object.
     */
    public abstract ICassandraDeepJobConfig<T> batchSize(int batchSize);

    /**
     * Returns the batch size used for writing objects to the underying Cassandra datastore.
     *
     * @return the batch size.
     */
    public abstract int getBatchSize();

    /**
     * Returns whether or not in this configuration object we specify to automatically create
     * the output column family.
     *
     * @return true if this configuration object has been configured to create missing tables on writes.
     */
    public abstract Boolean isCreateTableOnWrite();

    /**
     * Whether or not to create the output column family on write.<br/>.
     * <p/>
     * Defaults to FALSE.
     *
     * @param createTableOnWrite a boolean that tells this configuration obj to create missing tables on write.
     * @return this configuration object.
     */
    public abstract ICassandraDeepJobConfig<T> createTableOnWrite(Boolean createTableOnWrite);


    /**
     * Returns the name of the configured column family.
     *
     * @return the configured column family.
     */
    public abstract String getColumnFamily();

    /**
     * Sets the session to use. If a session is not provided, this object will open a new one.
     *
     * @param session the session to use.
     */
    public abstract ICassandraDeepJobConfig<T> session(Session session);
}
