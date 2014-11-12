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

package com.stratio.deep.mongodb.partition;

import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.commons.rdd.DeepTokenRange;

/**
 * Created by rcrespo on 7/11/14.
 */
public class MongoPartition extends DeepPartition {

    /**
     * The constant serialVersionUID.
     */
    private static final long serialVersionUID = 3740901434550932793L;

    /**
     * The Key.
     */
    private String key = "_id";

    /**
     * Instantiates a new Mongo partition.
     *
     * @param rddId the rdd id
     * @param idx   the idx
     * @param range the range
     * @param key   the key
     */
    public MongoPartition(int rddId, int idx, DeepTokenRange range, String key) {
        super(rddId, idx, range);
        this.key = key;
    }

    /**
     * Gets key.
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets key.
     *
     * @param key the key
     */
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer(super.toString() + "MongoPartition{");
        sb.append("key='").append(key).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
