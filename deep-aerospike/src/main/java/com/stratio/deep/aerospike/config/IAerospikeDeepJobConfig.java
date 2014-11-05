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

package com.stratio.deep.aerospike.config;

import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Defines the public methods that each Stratio Deep Aerospike configuration object should implement.
 */
public interface IAerospikeDeepJobConfig<T> {

    /**
     * Aerospike's set name
     */
    IAerospikeDeepJobConfig<T> set(String set);

    /**
     * Aerospike's namespace
     */
    IAerospikeDeepJobConfig<T> namespace(String namespace);

    String getNamespace();

    String getSet();

    /**
     * Aerospike's bin name
     */
    IAerospikeDeepJobConfig<T> bin(String bin);

    String getBin();

    /**
     * @return the hadoop configuration object if the concrete implementation has one, null otherwise.
     */
    Configuration getHadoopConfiguration();

    /**
     * Sets the list of available Aerospike hosts.
     *
     * @param host the list of available mongo hosts.
     * @return this object.
     */
    IAerospikeDeepJobConfig<T> host(List<String> host);

    /**
     * Sets the list of available Aerospike ports.
     * @param port
     * @return
     */
    IAerospikeDeepJobConfig<T> port(List<Integer> port);

    /**
     * Sets the operation to perform in Aerospike.
     *
     * @param operation the operation to perform in aerospike.
     * @return
     */
    IAerospikeDeepJobConfig<T> operation(String operation);

    String getOperation();

}
