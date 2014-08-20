package com.stratio.deep.rdd;

import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.utils.Pair;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.stratio.deep.config.IDeepJobConfig;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IDeepRDD<T> extends Serializable{



    Partition[] getPartitions(DeepJobConfig<T> config, int id);

    Iterator<T> compute(TaskContext context, IDeepPartition partition, DeepJobConfig<T> config);


    boolean hasNext();

    T next();

    void close();

    void initIterator(final IDeepPartition dp,
                 DeepJobConfig<T> config);

  // TODO Implement and document
  // void write(Cells rawKey, Cells data);
}
