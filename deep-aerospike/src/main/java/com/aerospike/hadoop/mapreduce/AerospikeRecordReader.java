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

import com.aerospike.client.*;
import com.aerospike.client.AerospikeException.ScanTerminated;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AerospikeRecordReader
    extends RecordReader<AerospikeKey, AerospikeRecord>
    implements org.apache.hadoop.mapred.RecordReader<AerospikeKey,
                                                         AerospikeRecord> {

    private class KeyRecPair {
        public AerospikeKey key;
        public AerospikeRecord rec;
        public KeyRecPair(AerospikeKey key, AerospikeRecord rec) {
            this.key = key;
            this.rec = rec;
        }
    }

    private static final Log log =
        LogFactory.getLog(AerospikeRecordReader.class);

    private ASSCanReader scanReader = null;
    private ASQueryReader queryReader = null;

    private ArrayBlockingQueue<KeyRecPair> queue = new ArrayBlockingQueue<>(16 * 1024);

    volatile private boolean isFinished = false;
    volatile private boolean isError = false;
    volatile private boolean isRunning = false;

    private String numrangeBin;
    private long numrangeBegin;
    private long numrangeEnd;
    
    private AerospikeKey currentKey;
    private AerospikeRecord currentValue;

    public class CallBack implements ScanCallback {
        @Override
        public void scanCallback(Key key, Record record) throws AerospikeException {
            try {
                queue.put(new KeyRecPair(new AerospikeKey(key), new AerospikeRecord(record)));
            } catch (Exception ex) {
                throw new ScanTerminated(ex);
            }
        }
    }

    public class ASSCanReader extends java.lang.Thread {

        String node;
        String host;
        int port;
        String namespace;
        String setName;
        String[] binNames;

        ASSCanReader(String node, String host, int port, String ns, String setName, String[] binNames) {
            this.node = node;
            this.host = host;
            this.port = port;
            this.namespace = ns;
            this.setName = setName;
            this.binNames = binNames;
        }

        public void run() {
            try {
                AerospikeClient client = AerospikeClientSingleton.getInstance(new ClientPolicy(), host, port);

                log.info(String.format("scanNode %s:%d:%s:%s", host, port, namespace, setName));
                ScanPolicy scanPolicy = new ScanPolicy();
                CallBack cb = new CallBack();
                log.info("scan starting");
                isRunning = true;
                if (binNames != null) 
                    client.scanNode(scanPolicy, node, namespace, setName, cb, binNames);
                else
                    client.scanNode(scanPolicy, node, namespace, setName, cb);
                isFinished = true;
                log.info("scan finished");
            } catch (Exception ex) {
                log.error("exception in ASSCanReader.run: " + ex);
                isError = true;
            }
        }
    }

    public class ASQueryReader extends java.lang.Thread {

        String node;
        String host;
        int port;
        String namespace;
        String setName;
        String[] binNames;
        String numrangeBin;
        long numrangeBegin;
        long numrangeEnd;

        ASQueryReader(String node, String host, int port,
                      String ns, String setName, String[] binNames,
                      String numrangeBin, long numrangeBegin, long numrangeEnd) {
            this.node = node;
            this.host = host;
            this.port = port;
            this.namespace = ns;
            this.setName = setName;
            this.binNames = binNames;
            this.numrangeBin = numrangeBin;
            this.numrangeBegin = numrangeBegin;
            this.numrangeEnd = numrangeEnd;
        }

        public void run() {
            try {
                AerospikeClient client = AerospikeClientSingleton.getInstance(new ClientPolicy(), host, port);
                log.info(String.format("queryNode %s:%d %s:%s:%s[%d:%d]", host, port, namespace, setName,
                                       numrangeBin, numrangeBegin, numrangeEnd));
                Statement stmt = new Statement();
                stmt.setNamespace(namespace);
                stmt.setSetName(setName);
                stmt.setFilters(Filter.range(numrangeBin, numrangeBegin, numrangeEnd));
                if (binNames != null)
                    stmt.setBinNames(binNames);
                QueryPolicy queryPolicy = new QueryPolicy();
                RecordSet rs = client.queryNode(queryPolicy, stmt, client.getNode(node));
                isRunning = true;
                try {
                    log.info("query starting");
                    while (rs.next()) {
                        Key key = rs.getKey();
                        Record record = rs.getRecord();
                        queue.put(new KeyRecPair(new AerospikeKey(key), new AerospikeRecord(record)));
                    }
                } finally {
                    rs.close();
                    isFinished = true;
                    log.info("query finished");
                }
            } catch (Exception ex) {
                isError = true;
            }
        }
    }

    public AerospikeRecordReader() throws IOException {
        log.info("NEW CTOR");
    }

    public AerospikeRecordReader(AerospikeSplit split) throws IOException {
        log.info("OLD CTOR");
        init(split);
    }

    public void init(AerospikeSplit split) throws IOException {
        final String type = split.getType();
        final String node = split.getNode();
        final String host = split.getHost();
        final int port = split.getPort();
        final String namespace = split.getNameSpace();
        final String setName = split.getSetName();
        final String[] binNames = split.getBinNames();
        this.numrangeBin = split.getNumRangeBin();
        this.numrangeBegin = split.getNumRangeBegin();
        this.numrangeEnd = split.getNumRangeEnd();

        if (type.equals("scan")) {
            scanReader = new ASSCanReader(node, host, port, namespace, setName, binNames);
            scanReader.start();
        } else if (type.equals("numrange")) {
            queryReader = new ASQueryReader(node, host, port, namespace, setName, binNames, numrangeBin,
                                            numrangeBegin, numrangeEnd);
            queryReader.start();
        }

        log.info("node: " + node);
    }

    public AerospikeKey createKey() { return new AerospikeKey(); }

    public AerospikeRecord createValue() { return new AerospikeRecord(); }

    protected void setCurrentKey(AerospikeKey container, AerospikeKey actual) {
        currentKey = container;
        container.set(actual);
    }

    protected void setCurrentValue(AerospikeRecord container, AerospikeRecord actual) {
        currentValue = container;
        container.set(actual);
    }

    public synchronized boolean next(AerospikeKey key, AerospikeRecord value) throws IOException {
        try {
            if (isError) return false;

            while (!isRunning) {
                Thread.sleep(100);
            }

            KeyRecPair pair = queue.poll(5, TimeUnit.SECONDS);
            if (! isFinished) {
                if(pair == null) {
                    log.error("SCAN TIMEOUT");
                    return false;
                }
            } else if (pair == null) {
                return false;
            }

            // log.info("key=" + pair.key + ", val=" + pair.rec);
            setCurrentKey(key, pair.key);
            setCurrentValue(value, pair.rec);
        } catch (Exception ex) {
            log.error("exception in AerospikeRecordReader.next: " + ex);
            throw new IOException("exception in AerospikeRecordReader.next", ex);
        }
        return true;
    }


    public float getProgress() {
        return isFinished ? 1.0f : 0.0f;
    }

    public long getPos() throws IOException {
        return 0;
    }

    public synchronized void close() throws IOException {
        if (scanReader != null) {
            try {
                scanReader.join();
            } catch (Exception ex) {
                throw new IOException("exception in AerospikeRecordReader.close", ex);
            }
            scanReader = null;
        }
        if (queryReader != null) {
            try {
                queryReader.join();
            } catch (Exception ex) {
                throw new IOException("exception in AerospikeRecordReader.close", ex);
            }
            queryReader = null;
        }
    }

    // ---------------- NEW API ----------------

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        log.info("INITIALIZE");
        init((AerospikeSplit) split);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        AerospikeKey key = currentKey != null ? currentKey : createKey();
        AerospikeRecord value = currentValue != null ? currentValue : createValue();
        // FIXME: does the new API mandate a new instance each time (?)
        return next(key, value);
    }

    @Override
    public AerospikeKey getCurrentKey() throws IOException {
        return currentKey;
    }

    @Override
    public AerospikeRecord getCurrentValue() {
        return currentValue;
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
