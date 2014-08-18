package com.stratio.deep.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.utils.Pair;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by rcrespo on 13/08/14.
 */
public interface IDeepHadoopRDD<T, K, V> extends Serializable {


    T transformElement(Tuple2<K, V> tuple, Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?,?>>> config);


}
