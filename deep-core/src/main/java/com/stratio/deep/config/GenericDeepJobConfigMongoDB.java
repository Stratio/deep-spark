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

import com.mongodb.hadoop.util.MongoConfigUtil;
import com.stratio.deep.entity.Cell;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @param <T>
 */
public class GenericDeepJobConfigMongoDB<T> implements IMongoDeepJobConfig<T> {
    private static final Logger LOG = Logger.getLogger("com.stratio.deep.config.GenericDeepJobConfigMongoDB");
    private static final long serialVersionUID = -7179376653643603038L;

    /**
     *
     */
    public Configuration configHadoop;

    /**
     * A list of mongodb host to connect
     */
    private List<String> hostList = new ArrayList<>();


    /**
     * MongoDB username
     */
    private String username;

    /**
     * MongoDB password
     */

    private String password;

    /**
     * Indicates the replica set's name
     */
    private String replicaSet;

    /**
     * Collection to get or insert data
     */
    private String collection;

    /**
     * Database to connect
     */
    private String database;

    /**
     * Read Preference
     */
    private String readPreference;

    /**
     * Entity class to map BSONObject
     */
    protected Class<T> entityClass;

    /**
     * VIP, this MUST be transient!
     */
    private transient Map<String, Cell> columnDefinitionMap;

    /**
     * Default constructor
     */
    public GenericDeepJobConfigMongoDB() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Cell> columnDefinitions() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> filterByField(String filterColumnName, Serializable filterValue) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    // TODO
    @Override
    public IMongoDeepJobConfig<T> pageSize(int pageSize) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHost() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    // TODO
    @Override
    public String[] getInputColumns() {
        return new String[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getPassword() {
        return password;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUsername() {
        return username;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> host(String host) {
        this.hostList.add(host);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> host(List<String> host) {
        this.hostList.addAll(host);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T>replicaSet(String replicaSet) {
        this.replicaSet = replicaSet;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> database(String database) {
        this.database = database;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> collection(String collection) {
        this.collection = collection;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> username(String username) {
        this.username = username;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    // TODO
    @Override
    public Map<String, Serializable> getAdditionalFilters() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    // TODO
    @Override
    public int getPageSize() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> password(String password) {
        this.password = password;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMongoDeepJobConfig<T> readPreference(String readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericDeepJobConfigMongoDB<T> initialize() {
        validate();
        configHadoop = new Configuration();
        StringBuilder connection = new StringBuilder();

        connection.append("mongodb").append(":").append("//");

        if (username != null && password != null) {
            connection.append(username).append(":").append(password).append("@");
        }

        boolean firstHost = true;
        for (String host : hostList) {
            if (!firstHost) {
                connection.append(",");
            }
            connection.append(host);
            firstHost = false;
        }

        connection.append("/").append(database).append(".").append(collection);

        StringBuilder options = new StringBuilder();
        boolean asignado = false;


        if (readPreference != null) {
            asignado = true;
            options.append("?readPreference=").append(readPreference);
        }

        if (replicaSet != null) {
            if (asignado) {
                options.append("&");
            } else {
                options.append("?");
            }
            options.append("replicaSet=").append(replicaSet);
        }

        connection.append(options);

        configHadoop.set(MongoConfigUtil.INPUT_URI, connection.toString());
        configHadoop.set(MongoConfigUtil.OUTPUT_URI, connection.toString());

        // it allows hadoop to make data split
        configHadoop.set(MongoConfigUtil.CREATE_INPUT_SPLITS, "false");

        if (username != null && password != null) {
            //TODO: In release 1.2.1 mongo-hadoop will have a new feature with mongos process.
//            configHadoop.set(MongoConfigUtil.AUTH_URI , connection.toString());
        }

        return this;
    }

    /**
     * validates connection parameters
     */
    private void validate(){
        if(hostList.isEmpty()){
            throw new IllegalArgumentException("host cannot be null");
        }
        if(database==null){
            throw new IllegalArgumentException("database cannot be null");
        }
        if(collection==null){
            throw new IllegalArgumentException("collection cannot be null");
        }
    }

    /**
     * {@inheritDoc}
     */
    // TODO
    @Override
    public IMongoDeepJobConfig<T> inputColumns(String... columns) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getHadoopConfiguration() {
        return configHadoop;
    }
}
