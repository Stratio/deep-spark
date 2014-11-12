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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

public class AerospikeConfigUtil {
    private static final Log log = LogFactory.getLog(AerospikeConfigUtil.class);

    // ---------------- INPUT ----------------

    public static final String INPUT_HOST = "aerospike.input.host";
    public static final String DEFAULT_INPUT_HOST = "localhost";
    public static final String INPUT_PORT = "aerospike.input.port";
    public static final int DEFAULT_INPUT_PORT = 3000;
    public static final String INPUT_NAMESPACE = "aerospike.input.namespace";
    public static final String INPUT_SETNAME = "aerospike.input.setname";
    public static final String INPUT_OPERATION = "aerospike.input.operation";
    public static final String DEFAULT_INPUT_OPERATION = "scan";
    public static final String INPUT_NUMRANGE_BIN = "aerospike.input.numrange.bin";
    public static final String INPUT_NUMRANGE_BEGIN = "aerospike.input.numrange.begin";
    public static final String INPUT_NUMRANGE_END = "aerospike.input.numrange.end";
    public static final long INVALID_LONG = 762492121482318889L;

    // ---------------- OUTPUT ----------------

    public static final String OUTPUT_HOST = "aerospike.output.host";
    public static final String DEFAULT_OUTPUT_HOST = "localhost";
    public static final String OUTPUT_PORT = "aerospike.output.port";
    public static final int DEFAULT_OUTPUT_PORT = 3000;
    public static final String OUTPUT_NAMESPACE = "aerospike.output.namespace";
    public static final String OUTPUT_SETNAME = "aerospike.output.setname";
    public static final String OUTPUT_BINNAME = "aerospike.output.binname";
    public static final String OUTPUT_KEYNAME = "aerospike.output.keyname";

    // ---------------- INPUT ----------------

    public static void setInputHost(Configuration conf, String host) {
        log.info("setting " + INPUT_HOST + " to " + host);
        conf.set(INPUT_HOST, host);
    }

    public static String getInputHost(Configuration conf) {
        String host = conf.get(INPUT_HOST, DEFAULT_INPUT_HOST);
        log.info("using " + INPUT_HOST + " = " + host);
        return host;
    }

    public static void setInputPort(Configuration conf, int port) {
        log.info("setting " + INPUT_PORT + " to " + port);
        conf.setInt(INPUT_PORT, port);
    }

    public static int getInputPort(Configuration conf) {
        int port = conf.getInt(INPUT_PORT, DEFAULT_INPUT_PORT);
        log.info("using " + INPUT_PORT + " = " + port);
        return port;
    }

    public static void setInputNamespace(Configuration conf, String namespace) {
        log.info("setting " + INPUT_NAMESPACE + " to " + namespace);
        conf.set(INPUT_NAMESPACE, namespace);
    }

    public static String getInputNamespace(Configuration conf) {
        String namespace = conf.get(INPUT_NAMESPACE);
        if (namespace == null)
            throw new UnsupportedOperationException
                ("you must set the input namespace");
        log.info("using " + INPUT_NAMESPACE + " = " + namespace);
        return namespace;
    }

    public static void setInputSetName(Configuration conf, String setname) {
        log.info("setting " + INPUT_SETNAME + " to " + setname);
        conf.set(INPUT_SETNAME, setname);
    }

    public static String getInputSetName(Configuration conf) {
        String setname = conf.get(INPUT_SETNAME);
        log.info("using " + INPUT_SETNAME + " = " + setname);
        return setname;
    }

    public static void setInputOperation(Configuration conf, String operation) {
        if (!operation.equals("scan") &&
                !operation.equals("numrange"))
            throw new UnsupportedOperationException
                ("input operation must be 'scan' or 'numrange'");
        log.info("setting " + INPUT_OPERATION + " to " + operation);
        conf.set(INPUT_OPERATION, operation);
    }

    public static String getInputOperation(Configuration conf) {
        String operation = conf.get(INPUT_OPERATION, DEFAULT_INPUT_OPERATION);
        if (!operation.equals("scan") &&
                !operation.equals("numrange"))
            throw new UnsupportedOperationException
                ("input operation must be 'scan' or 'numrange'");
        log.info("using " + INPUT_OPERATION + " = " + operation);
        return operation;
    }

    public static void setInputNumRangeBin(Configuration conf, String binname) {
        log.info("setting " + INPUT_NUMRANGE_BIN + " to " + binname);
        conf.set(INPUT_NUMRANGE_BIN, binname);
    }

    public static String getInputNumRangeBin(Configuration conf) {
        String binname = conf.get(INPUT_NUMRANGE_BIN);
        log.info("using " + INPUT_NUMRANGE_BIN + " = " + binname);
        return binname;
    }

    public static void setInputNumRangeBegin(Configuration conf, long begin) {
        log.info("setting " + INPUT_NUMRANGE_BEGIN + " to " + begin);
        conf.setLong(INPUT_NUMRANGE_BEGIN, begin);
    }

    public static long getInputNumRangeBegin(Configuration conf) {
        long begin = conf.getLong(INPUT_NUMRANGE_BEGIN, INVALID_LONG);
        if (begin == INVALID_LONG && getInputOperation(conf).equals("numrange"))
            throw new UnsupportedOperationException
                ("missing input numrange begin");
        log.info("using " + INPUT_NUMRANGE_BEGIN + " = " + begin);
        return begin;
    }

    public static void setInputNumRangeEnd(Configuration conf, long end) {
        log.info("setting " + INPUT_NUMRANGE_END + " to " + end);
        conf.setLong(INPUT_NUMRANGE_END, end);
    }

    public static long getInputNumRangeEnd(Configuration conf) {
        long end = conf.getLong(INPUT_NUMRANGE_END, INVALID_LONG);
        if (end == INVALID_LONG && getInputOperation(conf).equals("numrange"))
            throw new UnsupportedOperationException
                ("missing input numrange end");
        log.info("using " + INPUT_NUMRANGE_END + " = " + end);
        return end;
    }

    // ---------------- OUTPUT ----------------

    public static void setOutputHost(Configuration conf, String host) {
        log.info("setting " + OUTPUT_HOST + " to " + host);
        conf.set(OUTPUT_HOST, host);
    }

    public static String getOutputHost(Configuration conf) {
        String host = conf.get(OUTPUT_HOST, DEFAULT_OUTPUT_HOST);
        log.info("using " + OUTPUT_HOST + " = " + host);
        return host;
    }

    public static void setOutputPort(Configuration conf, int port) {
        log.info("setting " + OUTPUT_PORT + " to " + port);
        conf.setInt(OUTPUT_PORT, port);
    }

    public static int getOutputPort(Configuration conf) {
        int port = conf.getInt(OUTPUT_PORT, DEFAULT_OUTPUT_PORT);
        log.info("using " + OUTPUT_PORT + " = " + port);
        return port;
    }

    public static void setOutputNamespace(Configuration conf, String namespace) {
        log.info("setting " + OUTPUT_NAMESPACE + " to " + namespace);
        conf.set(OUTPUT_NAMESPACE, namespace);
    }

    public static String getOutputNamespace(Configuration conf) {
        String namespace = conf.get(OUTPUT_NAMESPACE);
        if (namespace == null)
            throw new UnsupportedOperationException
                ("you must set the output namespace");
        log.info("using " + OUTPUT_NAMESPACE + " = " + namespace);
        return namespace;
    }

    public static void setOutputSetName(Configuration conf, String setname) {
        log.info("setting " + OUTPUT_SETNAME + " to " + setname);
        conf.set(OUTPUT_SETNAME, setname);
    }

    public static String getOutputSetName(Configuration conf) {
        String setname = conf.get(OUTPUT_SETNAME);
        log.info("using " + OUTPUT_SETNAME + " = " + setname);
        return setname;
    }

    public static void setOutputBinName(Configuration conf, String binname) {
        log.info("setting " + OUTPUT_BINNAME + " to " + binname);
        conf.set(OUTPUT_BINNAME, binname);
    }

    public static String getOutputBinName(Configuration conf) {
        String binname = conf.get(OUTPUT_BINNAME);
        log.info("using " + OUTPUT_BINNAME + " = " + binname);
        return binname;
    }

    public static void setOutputKeyName(Configuration conf, String keyname) {
        log.info("setting " + OUTPUT_KEYNAME + " to " + keyname);
        conf.set(OUTPUT_KEYNAME, keyname);
    }

    public static String getOutputKeyName(Configuration conf) {
        String keyname = conf.get(OUTPUT_KEYNAME);
        log.info("using " + OUTPUT_KEYNAME + " = " + keyname);
        return keyname;
    }

    // ---------------- COMMON ----------------

    public static org.apache.hadoop.mapred.JobConf asJobConf(Configuration cfg) {
        return cfg instanceof org.apache.hadoop.mapred.JobConf
            ? (org.apache.hadoop.mapred.JobConf) cfg
            : new org.apache.hadoop.mapred.JobConf(cfg);
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
