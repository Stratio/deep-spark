package com.stratio.deep.core.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by rcrespo on 13/08/14.
 */
public interface IDeepHadoopRDD<T, K, V> extends Serializable {


    T transformElement(Tuple2<K, V> tuple, Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>> config);


}
