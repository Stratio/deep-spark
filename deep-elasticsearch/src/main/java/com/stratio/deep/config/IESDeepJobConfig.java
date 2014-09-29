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

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.stratio.deep.commons.config.IDeepJobConfig;

/**
 * Defines the public methods that each Stratio Deep ES configuration object should implement.
 */
public interface IESDeepJobConfig<T> extends IDeepJobConfig<T, IESDeepJobConfig<T>> {
    /**
     * The ES's collection name
     */
    IESDeepJobConfig<T> collection(String collection);

    /**
     * The ES's database name
     */
    IESDeepJobConfig<T> database(String database);

    /**
     * Sets the list of available ElasticSearch hosts.
     *
     * @param host the list of available ElasticSearch hosts.
     * @return this object.
     */
    IESDeepJobConfig<T> host(List<String> host);

    /**
     * @return the hadoop configuration object if the concrete implementation has one, null otherwise.
     */
    Configuration getHadoopConfiguration();

    /**
     * Filter query
     *
     * @param query
     * @return this object.
     */
    IESDeepJobConfig<T> filterQuery(String query);

    /**
     * Sorting
     *
     * @param sort
     * @return this object.
     */
    IESDeepJobConfig<T> sort(String sort);

    /**
     * @param inputKey
     * @return this object.
     */
    IESDeepJobConfig<T> inputKey(String inputKey);

    /**
     * @return Hosts list
     */
    List<String> getHostList();

}
