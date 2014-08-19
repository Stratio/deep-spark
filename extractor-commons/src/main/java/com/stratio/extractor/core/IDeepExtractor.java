package com.stratio.extractor.core;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.rdd.IDeepRecordReader;
import com.stratio.deep.utils.Pair;
import org.apache.spark.Partition;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dgomez on 19/08/14.
 */
public interface IDeepExtractor<T> extends Serializable{

    Iterator<T> compute(IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader,
                        IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config);

    Partition[] getPartitions(IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config, int id);


//    IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> createRecordReader();

  // TODO Implement and document
  // void write(Cells rawKey, Cells data);
}
