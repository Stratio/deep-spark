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

package com.stratio.deep.commons.config;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;


/**
 * Defines the public methods that each Stratio Deep configuration object should implement.
 *
 * @param <T> the generic type associated to this configuration object.
 */
public interface IDeepJobConfig<T, S extends IDeepJobConfig<?, ?>> extends Serializable {

    /**
     * Returns the password needed to authenticate
     * to the remote datastore cluster.
     *
     * @return the password used to login to the remote cluster.
     */
    String getPassword();


    /**
     * Returns the hostname of the cassandra server.
     *
     * @return the endpoint of the cassandra server.
     */
    String getHost();

    /**
     * Returns the list of column names that will
     * be fetched from the underlying datastore.
     *
     * @return the array of column names that will be retrieved from the data store.
     */
    String[] getInputColumns();

    /**
     * Returns the username used to authenticate to the cassandra server.
     * Defaults to the empty string.
     *
     * @return the username to use to login to the remote server.
     */
    String getUsername();


    /**
     * Returns the maximum number of rows that will be retrieved when fetching data pages from Cassandra.
     *
     * @return the page size
     */
    int getPageSize();


    void setRddId(int rddId);

    void setPartitionId(int rddId);

    int getRddId();

    int getPartitionId();

}
