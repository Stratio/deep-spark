/* 
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.aerospike.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

public abstract class AerospikeRecordWriter<KK, VV>
    extends RecordWriter<KK, VV>
    implements org.apache.hadoop.mapred.RecordWriter<KK, VV> {

    private static final Log log =
        LogFactory.getLog(AerospikeRecordWriter.class);

    private static final int NO_TASK_ID = -1;

    protected final Configuration cfg;
    protected boolean initialized = false;

    private static String namespace;
    private static String setName;
    private static AerospikeClient client;
    private static WritePolicy writePolicy;

    private Progressable progressable;

    public AerospikeRecordWriter(Configuration cfg, Progressable progressable) {
        this.cfg = cfg;
        this.progressable = progressable;
    }

    public abstract void writeAerospike(KK key,
                                        VV value,
                                        AerospikeClient client,
                                        WritePolicy writePolicy,
                                        String namespace,
                                        String setName) throws IOException;

    @Override
    public void write(KK key, VV value) throws IOException {
        if (!initialized) {
            initialized = true;
            init();
        }

        writeAerospike(key, value, client, writePolicy, namespace, setName);
    }

    protected void init() throws IOException {

        String host = AerospikeConfigUtil.getOutputHost(cfg);
        int port = AerospikeConfigUtil.getOutputPort(cfg);

        namespace = AerospikeConfigUtil.getOutputNamespace(cfg);
        setName = AerospikeConfigUtil.getOutputSetName(cfg);

        log.info(String.format("init: %s %d %s %s",
                               host, port, namespace, setName));

        ClientPolicy policy = new ClientPolicy();
        policy.user = "";
        policy.password = "";
        policy.failIfNotConnected = true;

        client = AerospikeClientSingleton.getInstance(policy, host, port);

        writePolicy = new WritePolicy();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        doClose(context);
    }

    @Override
    public void close(org.apache.hadoop.mapred.Reporter reporter
                      ) throws IOException {
        doClose(reporter);
    }

    protected void doClose(Progressable progressable) {
        log.info("doClose");
        initialized = false;
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
