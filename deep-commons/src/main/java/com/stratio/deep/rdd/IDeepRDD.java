package com.stratio.deep.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IDeepRDD<T> {


    Iterator<T> compute(Partition split, TaskContext ctx, Broadcast<IDeepJobConfig<T, IDeepJobConfig<T, ?>>> config);

    Partition[] getPartitions(Broadcast<IDeepJobConfig<T, IDeepJobConfig<T, ?>>> config, int id);
}

