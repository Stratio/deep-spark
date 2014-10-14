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
 */
public class DeepJobConfig<T> extends BaseConfig<T> implements IDeepJobConfig<T, DeepJobConfig<T>>, Serializable {

    protected String username;

    protected String password;

    protected String nameSpace;

    protected String catalog;

    protected String table;

    protected int pageSize;

    protected int port;

    protected String host;

    protected String[] fields;


    public DeepJobConfig(){
//        super(Cells.class);
        super(null);
    }

    public DeepJobConfig(Class<T> t) {
        super(t);
    }

    public String getUsername() {
        return username;
    }

//    @Override
    public DeepJobConfig host(String hostname) {
        return this;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

//    @Override
    public DeepJobConfig pageSize(int pageSize) {
        return null;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNameSpace(){
        if (nameSpace==null){
            nameSpace = new StringBuilder().append(catalog).append(".").append(table).toString();
        }
        return nameSpace;

    }


    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String... fields) {
        this.fields = fields;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String[] getInputColumns() {
        return new String[0];
    }

    public void setHost(String host) {
        this.host = host;
    }

    public <S extends DeepJobConfig> S initialize(ExtractorConfig extractorConfig){
        return null;
    }

//    @Override
    public <S extends DeepJobConfig> S inputColumns(String... columns) {
        return null;
    }

//    @Override
    public <S extends DeepJobConfig> S password(String password) {
        return null;
    }

//    @Override
    public <S extends DeepJobConfig> S username(String username) {
        return null;
    }

    public <S extends DeepJobConfig> S initialize(){
        return null;
    }
}
