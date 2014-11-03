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

import com.stratio.deep.aerospike.extractor.AerospikeCellExtractor;
import com.stratio.deep.aerospike.extractor.AerospikeEntityExtractor;
import com.stratio.deep.commons.config.HadoopConfig;
import com.stratio.deep.commons.entity.Cells;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AerospikeDeepJobConfig<T> extends HadoopConfig<T> implements IAerospikeDeepJobConfig<T>, Serializable{

    private static final long serialVersionUID = 2778930913494063818L;

    /**
     * configuration to be broadcasted to every spark node
     */
    private transient Configuration configHadoop;

    /**
     * A list of aerospike hosts to connect
     */
    private List<String> hostList = new ArrayList<>();

    /**
     * Aerospike's set name
     */
    private String set;

    /**
     * Aerospike's bin name
     */
    private String bin;

    public AerospikeDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
        if(Cells.class.isAssignableFrom(entityClass)){
            extractorImplClass = AerospikeCellExtractor.class;
        }else{
            extractorImplClass = AerospikeEntityExtractor.class;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHost() {
        return !hostList.isEmpty() ? hostList.get(0) : null;
    }

    public List<String> getHostList() {
        return hostList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AerospikeDeepJobConfig<T> host(String host) {
        this.hostList.add(host);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AerospikeDeepJobConfig<T> host(List<String> host) {
        this.hostList.addAll(host);
        return this;
    }

    public AerospikeDeepJobConfig<T> host(String[] hosts) {
        this.hostList.addAll(Arrays.asList(hosts));
        return this;
    }

    @Override
    public IAerospikeDeepJobConfig<T> set(String set) {
        this.set = set;
        return this;
    }

    @Override
    public String getSet() {
        return this.set;
    }

    @Override
    public IAerospikeDeepJobConfig<T> bin(String bin) {
        this.bin = bin;
        return this;
    }

    @Override
    public String getBin() {
        return this.bin;
    }

}
