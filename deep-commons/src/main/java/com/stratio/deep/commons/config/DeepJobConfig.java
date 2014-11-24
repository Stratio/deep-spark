/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.commons.config;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.COLLECTION;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.DATABASE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.HOST;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.INPUT_COLUMNS;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.PASSWORD;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.PORT;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USERNAME;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.Filter;

/**
 * Created by rcrespo on 13/10/14.
 * @param <T>  the type parameter
 */
public class DeepJobConfig<T, S> extends BaseConfig<T> implements Serializable {

    /**
     * The Username.
     */
    protected String username;

    /**
     * The Password.
     */
    protected String password;

    /**
     * The Name space (catalog+table / database+collection).
     */
    protected String nameSpace;

    /**
     * The Catalog / database.
     */
    protected String catalog;

    /**
     * The Table / collection.
     */
    protected String table;

    /**
     * The Port.
     */
    protected int port;

    /**
     * The Host.
     */
    protected List<String> host = new ArrayList<>();

    /**
     * The Fields.
     */
    protected String[] fields;


    /**
     * Database filters
     */
    protected Filter[] filters;

    /**
     * Instantiates a new Deep job config.
     */
    public DeepJobConfig(){
        super((Class<T>) Cells.class);
    }

    /**
     * Instantiates a new Deep job config.
     *
     * @param t the t
     */
    public DeepJobConfig(Class<T> t) {
        super(t);
    }

    /**
     * Gets username.
     *
     * @return the username
     */
    public String getUsername() {
        return username;
    }


    /**
     * Gets password.
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }



    /**
     * Get name space.
     *
     * @return the string
     */
    public String getNameSpace(){
        if (nameSpace==null){
            nameSpace = new StringBuilder().append(catalog).append(".").append(table).toString();
        }
        return nameSpace;

    }

    /**
     * Gets catalog.
     *
     * @return the catalog
     */
    public String getCatalog() {
        return catalog;
    }


    /**
     * Gets table.
     *
     * @return the table
     */
    public String getTable() {
        return table;
    }


    /**
     * Get fields.
     *
     * @return the string [ ]
     */
    public String[] getFields() {
        return fields;
    }


    /**
     * Gets port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }


    /**
     * Gets host.
     *
     * @return the host
     */
    public List<String> getHostList() {
        return host;
    }

    public String getHost() {
        return !host.isEmpty() ? host.get(0) : null;
    }


    /**
     * Initialize s.
     *
     
     * @param extractorConfig the extractor config
     * @return the s
     */
    public S initialize(ExtractorConfig extractorConfig){
        Map<String, Serializable> values = extractorConfig.getValues();

        if (values.get(USERNAME) != null) {
            username(extractorConfig.getString(USERNAME));
        }

        if (values.get(PASSWORD) != null) {
            password(extractorConfig.getString(PASSWORD));
        }

        if (values.get(HOST) != null) {
            host((extractorConfig.getStringArray(HOST)));
        }

        if (values.get(PORT) != null) {
            port((extractorConfig.getInteger(PORT)));
        }

        if (values.get(COLLECTION) != null) {
            table(extractorConfig.getString(COLLECTION));
        }

        if (values.get(INPUT_COLUMNS) != null) {
            fields(extractorConfig.getStringArray(INPUT_COLUMNS));
        }

        if (values.get(DATABASE) != null) {
            catalog(extractorConfig.getString(DATABASE));
        }
        return (S) this;
    }

    /**
     * Host deep job config.
     *
     * @param hostName the hostname
     * @return the deep job config
     */
    public DeepJobConfig host(String hostName) {
        host.add(hostName);
        return this;
    }

    public DeepJobConfig host(String... hostNames) {
        for(String hostName : hostNames)
        host.add(hostName);
        return this;
    }

    public DeepJobConfig host(List<String> hostNames) {
        this.host.addAll(hostNames);
        return this;
    }
    /**
     * Password s.
     *
     * @param password the password
     * @return the s
     */
    public S password(String password) {
        this.password=password;
        return (S) this;
    }


    public S table(String table) {
        this.table=table;
        return (S) this;
    }

    public S catalog(String catalog) {
        this.catalog=catalog;
        return (S) this;
    }

    /**
     * Username s.
     *
     * @param username the username
     * @return the s
     */
    public S username(String username) {
        this.username=username;
        return (S) this;
    }

    /**
     * Port s.
     *
     * @param port the username
     * @return the s
     */
    public S port(Integer port) {
        this.port=port;
        return (S) this;
    }

    /**
     *
     * @param fields
     * @return
     */
    public S fields(String[] fields) {
        this.fields=fields;
        return (S) this;
    }

    /**
     *
     * @param filters
     * @return
     */
    public S filters(Filter[] filters) {
        this.filters=filters;
        return (S) this;
    }



    /**
     * Initialize s.
     * @return the s
     */
    public S initialize(){
        return (S) this;
    }
}
