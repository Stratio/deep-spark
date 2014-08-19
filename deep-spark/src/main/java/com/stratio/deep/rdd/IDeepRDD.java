package com.stratio.deep.rdd;

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

    Iterator<T> compute(IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader,
                        IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config);

    Partition[] getPartitions(IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config, int id);


//    IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> createRecordReader();

  // TODO Implement and document
  // void write(Cells rawKey, Cells data);
}
