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

package com.stratio.deep.es.config;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Defines the public methods that each Stratio Deep ES configuration object should implement.
 */
public interface IESDeepJobConfig<T> {

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

    ESDeepJobConfig<T> type(String type);

    ESDeepJobConfig<T> index(String index);

    String getType();

    String getIndex();

    /**
     * In case you want to add custom configuration, this could override any other configuration,
     * these configurations are not validated, so be careful
     *
     * @param customConfiguration
     * @return the IESDeepJobConfig.
     */
    IESDeepJobConfig<T> customConfiguration(Map<String, Serializable> customConfiguration);

    Map<String, Serializable> getCustomConfiguration();

}
