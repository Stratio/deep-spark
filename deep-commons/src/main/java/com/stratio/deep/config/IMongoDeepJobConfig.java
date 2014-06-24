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

import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.List;

/**
 * Created by luca on 23/06/14.
 */
public interface IMongoDeepJobConfig<T> extends IDeepJobConfig<T> {
    /**
     * {@inheritDoc}
     */
    @Override
    public abstract IMongoDeepJobConfig<T> password(String password);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract IMongoDeepJobConfig<T> username(String username);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract IMongoDeepJobConfig<T> inputColumns(String... columns);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract IMongoDeepJobConfig<T> host(String hostname);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract IMongoDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract IMongoDeepJobConfig<T> pageSize(int pageSize);

    /**
     * Creates necessary config to access mongoDB
     *
     * @return
     */
    @Override
    public abstract IMongoDeepJobConfig<T> initialize();

    /**
     * {@inheritDoc}
     */
    public abstract IMongoDeepJobConfig<T> collection(String collection);

    /**
     * {@inheritDoc}
     */
    public abstract IMongoDeepJobConfig<T> database(String database);

    /**
     * {@inheritDoc}
     */
    public abstract IMongoDeepJobConfig<T> host(List<String> host);

    /**
     * {@inheritDoc}
     */
    public abstract IMongoDeepJobConfig<T> replicaSet(String replicaSet);

    /**
     * @return the hadoop configuration object if the concrete implementation has one, null otherwise.
     */
    public abstract Configuration getHadoopConfiguration();

    /**
     * Configures the 'readPreference' MongoDB's config property.
     *
     * @param readPreference the property value to set.
     * @return this object.
     */
    public IMongoDeepJobConfig<T> readPreference(String readPreference);
}
