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
package com.stratio.deep.aerospike.writer;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper class for writing data into Aerospike.
 */
public class AerospikeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeWriter.class);

    private AerospikeClient aerospikeClient;

    private AerospikeDeepJobConfig deepJobConfig;

    private WritePolicy writePolicy;

    /**
     * Public constructor
     * @param deepJobConfig Aer
     */
    public AerospikeWriter(AerospikeDeepJobConfig deepJobConfig) {
        this.deepJobConfig = deepJobConfig;
        writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        init();
    }

    /**
     * Initialization method.
     */
    public void init() {
        List<Host> hosts = new ArrayList<>();
        Host aerospikeHost = new Host(deepJobConfig.getHost(), deepJobConfig.getAerospikePort());
        hosts.add(aerospikeHost);
        ClientPolicy clientPolicy = new ClientPolicy();
        if(deepJobConfig.getUsername() != null && deepJobConfig.getPassword() != null) {
            clientPolicy.user = deepJobConfig.getUsername();
            clientPolicy.password = deepJobConfig.getPassword();
        }
        aerospikeClient = new AerospikeClient(clientPolicy, hosts.toArray(new Host[hosts.size()]));
    }

    /**
     * Closes the Aerospike client connection.
     */
    public void close() {
        if(aerospikeClient != null) {
            aerospikeClient.close();
        }
    }

    /**
     * Saves a record into Aerospike.
     * @param record Record to save.
     */
    public void save(Record record) {
        Key key = new Key(deepJobConfig.getNamespace(), deepJobConfig.getSet(), Long.toString(System.nanoTime()));
        aerospikeClient.put(writePolicy, key, getBins(record));
    }

    private Bin[] getBins(Record record) {
        Bin[] bins = new Bin[record.bins.size()];
        int index = 0;
        for(Map.Entry<String, Object> bin:record.bins.entrySet()) {
            bins[index] = new Bin(bin.getKey(), bin.getValue());
            index++;
        }
        return bins;
    }


}
