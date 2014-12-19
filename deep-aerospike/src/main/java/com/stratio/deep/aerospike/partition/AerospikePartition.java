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
package com.stratio.deep.aerospike.partition;

import org.apache.spark.Partition;

/**
 * Created by mariomgal on 18/12/14.
 */
public class AerospikePartition implements Partition {

    private static final long serialVersionUID = -2444385819999809730L;

    private static final int MAGIC_NUMBER = 41;

    private int index;

    private String nodeHost;

    private int nodePort;

    public String getNodeHost() {
        return nodeHost;
    }

    public int getNodePort() {
        return nodePort;
    }

    public AerospikePartition(int index, String nodeHost, int nodePort) {
        this.index = index;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public int hashCode() {
        return MAGIC_NUMBER * (MAGIC_NUMBER + this.index) + this.index;
    }
}
