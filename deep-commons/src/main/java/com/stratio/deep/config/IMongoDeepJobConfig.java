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

import com.mongodb.QueryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.bson.BSONObject;

import java.io.Serializable;
import java.util.List;

/**
 * Defines the public methods that each Stratio Deep MongoDB configuration object should implement.
 */
public interface IMongoDeepJobConfig<T> extends IDeepJobConfig<T, IMongoDeepJobConfig<T>> {
    /**
     * The MongoDB's collection name
     */
    public abstract IMongoDeepJobConfig<T> collection(String collection);

    /**
     * The MongoDB's database name
     */
    public abstract IMongoDeepJobConfig<T> database(String database);

    /**
     * Sets the list of available Mongo hosts.
     * @param host the list of available mongo hosts.
     * @return this object.
     */
    public abstract IMongoDeepJobConfig<T> host(List<String> host);

    /**
     * The replica set identifier.
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
    public abstract IMongoDeepJobConfig<T> readPreference(String readPreference);

    /**
     *Filter query
     * @param query
     * @return
     */
    public IMongoDeepJobConfig<T> query(String query);

    /**
     *Filter query
     * @param query
     * @return
     */
    public IMongoDeepJobConfig<T> query(BSONObject query);

    /**
     *Filter query
     * @param query
     * @return
     */
    public IMongoDeepJobConfig<T> query(QueryBuilder query);

    /**
     *Fiels to be returned
     * @param fields
     * @return
     */
    public IMongoDeepJobConfig<T> fields(String fields);


    /**
     * Fiels to be returned
     * @param fields
     * @return
     */
    public IMongoDeepJobConfig<T> fields(BSONObject fields);

    /**
     * Sorting
     * @param sort
     * @return
     */
    public IMongoDeepJobConfig<T> sort(String sort);

    /**
     * Sorting
     * @param sort
     * @return
     */
    public IMongoDeepJobConfig<T> sort(BSONObject sort);
}
