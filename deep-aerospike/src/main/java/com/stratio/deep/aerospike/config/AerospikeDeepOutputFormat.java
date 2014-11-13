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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation for AerospikeOutputFormat.
 */
public class AerospikeDeepOutputFormat extends AerospikeOutputFormat<Object, AerospikeRecord> {

    /**
     * Hadoop record writer for Aerospike.
     */
    public static class AerospikeDeepRecordWriter extends AerospikeRecordWriter<Object, AerospikeRecord> {

        /**
         * Public constructor for AerospikeDeepRecordWriter.
         * @param cfg
         * @param progressable
         */
        public AerospikeDeepRecordWriter(Configuration cfg, Progressable progressable) {
            super(cfg, progressable);
        }

        @Override
        public void writeAerospike(Object key,
                                   AerospikeRecord record,
                                   AerospikeClient client,
                                   WritePolicy writePolicy,
                                   String namespace,
                                   String setName) throws IOException {
            //TODO -> How should I generate the key?
            Key k = new Key(namespace, setName, Long.toString(System.nanoTime()));
            List<Bin> bins = new ArrayList<>();
            for(Map.Entry<String, Object> bin:record.bins.entrySet()) {
                Bin aerospikeBin = new Bin(bin.getKey(), bin.getValue());
                bins.add(aerospikeBin);
            }
            client.put(writePolicy, k, bins.toArray(new Bin[bins.size()]));
        }
    }

    @Override
    public RecordWriter<Object, AerospikeRecord> getAerospikeRecordWriter(Configuration conf, Progressable progress) {
        return new AerospikeDeepRecordWriter(conf, progress);
    }

}
