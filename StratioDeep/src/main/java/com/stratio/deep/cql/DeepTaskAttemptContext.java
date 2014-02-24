package com.stratio.deep.cql;

import com.stratio.deep.config.impl.GenericDeepJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Created by luca on 24/02/14.
 */
public class DeepTaskAttemptContext<T> extends TaskAttemptContext {
    GenericDeepJobConfig<T> conf;

    public DeepTaskAttemptContext(Configuration conf, TaskAttemptID taskId) {
	super(conf, taskId);
    }

    public DeepTaskAttemptContext(GenericDeepJobConfig<T> conf, TaskAttemptID taskId) {
	super(conf.getConfiguration(), taskId);

	this.conf = conf;
    }
}
