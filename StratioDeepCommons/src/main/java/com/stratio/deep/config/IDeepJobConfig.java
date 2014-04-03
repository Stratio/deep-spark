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
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.Map;

/**
 * Defines the public methods that each Stratio Deep configuration object should implement.
 *
 * @param <T>
 */
public interface IDeepJobConfig<T> extends Serializable {

    /**
     * Returns the session opened to the cassandra server.
     * @return
     */
    Session getSession();

    /**
     * Sets the session to use. If a session is not provided, this object will open a new one.
     * @param session
     */
    IDeepJobConfig<T> session(Session session);

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
     * Column family name is case sensitive.
     *
     * @param columnFamily
     * @return
     */
    public abstract IDeepJobConfig<T> columnFamily(String columnFamily);

    public abstract IDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue);

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
     * Returns the underlying testentity class used to map the Cassandra
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
     *
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
     *
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
     *
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
     * @param partitionerClassName
     * @return this object.
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
     * Whether or not to create the output column family on write.<br/>.
     * <p/>
     * Defaults to FALSE.
     *
     * @param createTableOnWrite
     * @return
     */
    public abstract IDeepJobConfig<T> createTableOnWrite(Boolean createTableOnWrite);

    public abstract Boolean isCreateTableOnWrite();

    /**
     * Returns the configured read consistency level.
     *
     * @return
     */
    public abstract String getReadConsistencyLevel();

    /**
     * Returns the configured write consistency level.
     *
     * @return
     */
    public abstract String getWriteConsistencyLevel();

    /**
     * Sets read consistency level. <br/>
     * Can be one of those consistency levels defined in {@link org.apache.cassandra.db.ConsistencyLevel}.<br/>
     * Defaults to {@link org.apache.cassandra.db.ConsistencyLevel#LOCAL_ONE}.
     *
     * @param level
     * @return
     */
    public abstract IDeepJobConfig<T> readConsistencyLevel(String level);

    /**
     * Sets write consistency level. <br/>
     * Can be one of those consistency levels defined in {@link org.apache.cassandra.db.ConsistencyLevel}.<br/>
     * Defaults to {@link org.apache.cassandra.db.ConsistencyLevel#LOCAL_ONE}.
     *
     * @param level
     * @return
     */
    public abstract IDeepJobConfig<T> writeConsistencyLevel(String level);

    /**
     * Returns the name of the configured column family.
     * Column family name is case sensitive.
     *
     * @return
     */
    public abstract String getTable();

    /**
     * Returns the batch size used for writing objects to the underying Cassandra datastore.
     *
     * @return
     */
    int getBatchSize();
}
