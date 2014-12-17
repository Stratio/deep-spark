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

/**
 * Created by rcrespo on 11/12/14.
 */
public enum ExtractorType implements Serializable {

    /**
     * The MONGO_DB.
     */
    MONGO_DB("com.stratio.deep.mongodb.extractor.MongoNativeCellExtractor"),
    /**
     * The CASSANDRA.
     */
    CASSANDRA("com.stratio.deep.cassandra.extractor.CassandraCellExtractor"),
    /**
     * The ELASTIC_SEARCH.
     */
    ELASTIC_SEARCH ("com.stratio.deep.es.extractor.ESCellExtractor"),
    /**
     * The AEROSPIKE.
     */
    AEROSPIKE ("com.stratio.deep.aerospike.extractor.AerospikeCellExtractor");

    /**
     * The Value.
     */
    private final String value;

    /**
     * Instantiates a new Extractor type.
     *
     * @param value the value
     */
    ExtractorType (String value){
        this.value = value;

    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

}
