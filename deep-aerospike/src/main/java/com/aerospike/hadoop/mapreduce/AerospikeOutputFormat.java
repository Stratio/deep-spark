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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

public abstract class AerospikeOutputFormat<KK, VV>
    extends OutputFormat<KK, VV>
    implements org.apache.hadoop.mapred.OutputFormat<KK, VV> {

    private static final Log log =
        LogFactory.getLog(AerospikeOutputFormat.class);

    public static class AerospikeOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext)
            throws IOException {}

        // compatibility check with Hadoop 0.20.2
        @Deprecated
        public void cleanupJob(JobContext jobContext)
            throws IOException {}

        @Override
        public void setupTask(TaskAttemptContext taskContext)
            throws IOException {
            //no-op
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext)
            throws IOException {
            //no-op
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext)
            throws IOException {
            //no-op
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext)
            throws IOException {
            //no-op
        }

    }

    public static class AerospikeOldAPIOutputCommitter
        extends org.apache.hadoop.mapred.OutputCommitter {

        @Override
        public void setupJob(org.apache.hadoop.mapred.JobContext jobContext)
            throws IOException {
            //no-op
        }

        @Override
        public void setupTask(
            org.apache.hadoop.mapred.TaskAttemptContext taskContext)
            throws IOException {
            //no-op
        }

        @Override
        public boolean needsTaskCommit(
            org.apache.hadoop.mapred.TaskAttemptContext taskContext)
            throws IOException {
            //no-op
            return false;
        }

        @Override
        public void commitTask(
            org.apache.hadoop.mapred.TaskAttemptContext taskContext)
            throws IOException {
            //no-op
        }

        @Override
        public void abortTask(
            org.apache.hadoop.mapred.TaskAttemptContext taskContext)
            throws IOException {
            //no-op
        }

        @Override
        @Deprecated
        public void cleanupJob(org.apache.hadoop.mapred.JobContext context)
            throws IOException {
            // no-op
            // added for compatibility with hadoop 0.20.x (used by old
            // tools, such as Cascalog)
        }
    }

    public abstract org.apache.hadoop.mapred.RecordWriter<KK, VV>
        getAerospikeRecordWriter(Configuration conf, Progressable progress);
    
    //
    // new API - just delegates to the Old API
    //
    @SuppressWarnings("unchecked")
    @Override
    public RecordWriter<KK, VV> getRecordWriter(TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        return (RecordWriter<KK, VV>) getAerospikeRecordWriter(conf, context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        // careful as it seems the info here saved by in the config is discarded
        Configuration cfg = context.getConfiguration();
        init(cfg);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new AerospikeOutputCommitter();
    }

    //
    // old API
    //
    @Override
    public org.apache.hadoop.mapred.RecordWriter<KK, VV>
        getRecordWriter(FileSystem ignored,
                        org.apache.hadoop.mapred.JobConf job,
                        String name, Progressable progress) {
        return getAerospikeRecordWriter(job, progress);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored,
                                 org.apache.hadoop.mapred.JobConf cfg)
        throws IOException {
        init(cfg);
    }

    // NB: all changes to the config objects are discarded before the
    // job is submitted if _the old MR api_ is used
    private void init(Configuration cfg) throws IOException {
        log.info(String.format("init"));
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
