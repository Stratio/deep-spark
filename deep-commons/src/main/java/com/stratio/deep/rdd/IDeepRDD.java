package com.stratio.deep.rdd;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import scala.collection.Iterator;

import com.stratio.deep.config.IDeepJobConfig;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IDeepRDD<T> {

  Iterator<T> compute(Partition split, TaskContext ctx,
      IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config);

  Partition[] getPartitions(IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config, int id);

  // TODO Implement and document
  // void write(Cells rawKey, Cells data);
}
