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
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Map;

/**
 * Base class for all config implementations providing default implementations for methods
 * defined in {@link GenericDeepJobConfigMongoDB}.
 */
public class GenericDeepJobConfigMongoDB<T>  implements Serializable, IDeepJobConfig {
    private static final Logger LOG = Logger.getLogger("com.stratio.deep.config.GenericDeepJobConfigMongoDB");
    private static final long serialVersionUID = -7179376653643603038L;

    public Configuration configHadoop;


    /**
     * hostname of the cassandra server
     */
    private String host;


    /**
     * Cassandra username. Leave empty if you do not need authentication.
     */
    private String username;

    /**
     * Cassandra password. Leave empty if you do not need authentication.
     */
    private String password;


    private String port;

    private String collection;

    private String database;

    private String readPreference;

    private transient Map<String, Cell> columnDefinitionMap;

    public Map<String, Cell> getColumnDefinitions(){
        return columnDefinitionMap;
    }

    protected Class<T> entityClass;

    public GenericDeepJobConfigMongoDB(){

    }


    @Override
    public Session getSession() {
        return null;
    }

    @Override
    public IDeepJobConfig session(Session session) {
        return null;
    }

    @Override
    public Map<String, Cell> columnDefinitions() {
        return null;
    }

    @Override
    public IDeepJobConfig table(String table) {
        return null;
    }

    @Override
    public IDeepJobConfig columnFamily(String columnFamily) {
        return null;
    }

    @Override
    public IDeepJobConfig filterByField(String filterColumnName, Serializable filterValue) {
        return null;
    }

    @Override
    public IDeepJobConfig pageSize(int pageSize) {
        return null;
    }

    @Override
    public String getColumnFamily() {
        return null;
    }

    public Class<T> getEntityClass(){
    return entityClass;
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public String[] getInputColumns() {
        return new String[0];
    }

    @Override
    public String getKeyspace() {
        return null;
    }

    @Override
    public String getPartitionerClassName() {
        return null;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public Integer getRpcPort() {
        return null;
    }

    @Override
    public Integer getCqlPort() {
        return null;
    }

    @Override
    public String getUsername() {
        return null;
    }

    public GenericDeepJobConfigMongoDB<T> host(String host) {
        this.host=host;
        return this;
    }

    public GenericDeepJobConfigMongoDB<T> port(String port) {
        this.port=port;
        return this;
    }

    public GenericDeepJobConfigMongoDB<T> database(String database) {
        this.database=database;
        return this;
    }

    public GenericDeepJobConfigMongoDB<T> collection(String collection) {
        this.collection=collection;
        return this;
    }

    public GenericDeepJobConfigMongoDB<T> username(String username) {
        this.username=username;
        return this;
    }

    @Override
    public IDeepJobConfig batchSize(int batchSize) {
        return null;
    }

    @Override
    public IDeepJobConfig createTableOnWrite(Boolean createTableOnWrite) {
        return null;
    }

    @Override
    public Boolean isCreateTableOnWrite() {
        return null;
    }

    @Override
    public String getReadConsistencyLevel() {
        return null;
    }

    @Override
    public String getWriteConsistencyLevel() {
        return null;
    }

    @Override
    public IDeepJobConfig readConsistencyLevel(String level) {
        return null;
    }

    @Override
    public IDeepJobConfig writeConsistencyLevel(String level) {
        return null;
    }

    @Override
    public String getTable() {
        return null;
    }

    @Override
    public int getBatchSize() {
        return 0;
    }

    @Override
    public Map<String, Serializable> getAdditionalFilters() {
        return null;
    }

    @Override
    public int getPageSize() {
        return 0;
    }

    @Override
    public Boolean getIsWriteConfig() {
        return null;
    }

    @Override
    public int getBisectFactor() {
        return 0;
    }

    public GenericDeepJobConfigMongoDB<T> password(String password) {
        this.password=password;
        return this;
    }

    @Override
    public IDeepJobConfig rpcPort(Integer port) {
        return null;
    }

    @Override
    public IDeepJobConfig cqlPort(Integer port) {
        return null;
    }

    public GenericDeepJobConfigMongoDB<T> readPreference(String readPreference) {
        this.readPreference=readPreference;
        return this;
    }

    public GenericDeepJobConfigMongoDB(String host, String port, String database, String collection){
        this.host=host;
        this.port=port;
        this.database=database;
        this.collection=collection;
    }

    public GenericDeepJobConfigMongoDB<T> initialize() {

        configHadoop = new Configuration();
        StringBuilder connection = new StringBuilder();


        connection.append("mongodb").append(":").append("//");

        if(username!=null&&password!=null){
            connection.append(username).append(":").append(password).append("@");
        }
        connection.append(host).append(":").append(port).append("/").append(database).append(".").append(collection);



        if(readPreference!=null){
            connection.append("?readPreference=").append(readPreference);
        }

        configHadoop.set("mongo.input.uri", connection.toString());
        configHadoop.set("mongo.output.uri", connection.toString());

        return this;
    }

    @Override
    public IDeepJobConfig inputColumns(String... columns) {
        return null;
    }

    @Override
    public IDeepJobConfig keyspace(String keyspace) {
        return null;
    }

    @Override
    public IDeepJobConfig bisectFactor(int bisectFactor) {
        return null;
    }

    @Override
    public IDeepJobConfig partitioner(String partitionerClassName) {
        return null;
    }

    public void setEntityClass(Class<T> entityClass) {
        this.entityClass = entityClass;
    }


    public Configuration getHadoopConfiguration(){
        return configHadoop;
    }
}
