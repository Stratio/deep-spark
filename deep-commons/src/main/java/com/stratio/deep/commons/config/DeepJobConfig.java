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

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.stratio.deep.commons.entity.Cells;

/**
 * Created by rcrespo on 13/10/14.
 * @param <T>  the type parameter
 */
public class DeepJobConfig<T> extends BaseConfig<T> implements Serializable {

    /**
     * The Username.
     */
    protected String username;

    /**
     * The Password.
     */
    protected String password;

    /**
     * The Name space.
     */
    protected String nameSpace;

    /**
     * The Catalog.
     */
    protected String catalog;

    /**
     * The Table.
     */
    protected String table;

    /**
     * The Page size.
     */
    protected int pageSize;

    /**
     * The Port.
     */
    protected int port;

    /**
     * The Host.
     */
    protected String host;

    /**
     * The Fields.
     */
    protected String[] fields;

    /**
     * Instantiates a new Deep job config.
     */
    public DeepJobConfig(){
        super(null);
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
     * Host deep job config.
     *
     * @param hostname the hostname
     * @return the deep job config
     */
    public DeepJobConfig host(String hostname) {
        return this;
    }

    /**
     * Sets username.
     *
     * @param username the username
     */
    public void setUsername(String username) {
        this.username = username;
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
     * Page size.
     *
     * @param pageSize the page size
     * @return the deep job config
     */
    public DeepJobConfig pageSize(int pageSize) {
        return null;
    }

    /**
     * Sets password.
     *
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
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
     * Sets name space.
     *
     * @param nameSpace the name space
     */
    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
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
     * Sets catalog.
     *
     * @param catalog the catalog
     */
    public void setCatalog(String catalog) {
        this.catalog = catalog;
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
     * Sets table.
     *
     * @param table the table
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Gets page size.
     *
     * @return the page size
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets page size.
     *
     * @param pageSize the page size
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
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
     * Sets fields.
     *
     * @param fields the fields
     */
    public void setFields(String... fields) {
        this.fields = fields;
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
     * Sets port.
     *
     * @param port the port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets host.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Get input columns.
     *
     * @return the string [ ]
     */
    public String[] getInputColumns() {
        return new String[0];
    }

    /**
     * Sets host.
     *
     * @param host the host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Initialize s.
     *
     * @param <S>  the type parameter
     * @param extractorConfig the extractor config
     * @return the s
     */
    public <S extends DeepJobConfig> S initialize(ExtractorConfig extractorConfig){
        return null;
    }

    /**
     * Input columns.
     *
     * @param <S>  the type parameter
     * @param columns the columns
     * @return the s
     */
    public <S extends DeepJobConfig> S inputColumns(String... columns) {
        return null;
    }

    /**
     * Password s.
     *
     * @param <S>  the type parameter
     * @param password the password
     * @return the s
     */
    public <S extends DeepJobConfig> S password(String password) {
        return null;
    }

    /**
     * Username s.
     *
     * @param <S>  the type parameter
     * @param username the username
     * @return the s
     */
    public <S extends DeepJobConfig> S username(String username) {
        return null;
    }

    /**
     * Initialize s.
     * @param <S>  the type parameter
     * @return the s
     */
    public <S extends DeepJobConfig> S initialize(){
        return null;
    }
}
