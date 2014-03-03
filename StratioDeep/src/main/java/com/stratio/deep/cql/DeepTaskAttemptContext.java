package com.stratio.deep.cql;

import com.stratio.deep.config.impl.GenericDeepJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Custom {@link org.apache.hadoop.mapreduce.TaskAttemptContext} needed in order to
 * propagate a deep's own configuration object ({@link com.stratio.deep.config.impl.GenericDeepJobConfig}).
 */
public class DeepTaskAttemptContext<T> extends TaskAttemptContext
{
  GenericDeepJobConfig<T> conf;

  public DeepTaskAttemptContext(GenericDeepJobConfig<T> conf, TaskAttemptID taskId)
  {
    super(conf.getConfiguration(), taskId);

    this.conf = conf;
  }
}
