package com.stratio.deep.core.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.DeepHadoopRDD;
import scala.reflect.ClassTag;

/**
 * Created by rcrespo on 12/08/14.
 */
public class DeepGenericHadoopRDD<T, K, V> extends DeepHadoopRDD<T, K, V> {


    private IDeepHadoopRDD<T, K, V> innerDeepRDD;

    public DeepGenericHadoopRDD(SparkContext sc, IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> conf, Class<? extends org.apache.hadoop.mapreduce.InputFormat<K, V>> inputFormatClass, IDeepHadoopRDD<T, K, V> innerDeepRDD, ClassTag<T> evidence$1, ClassTag<K> evidence$2, ClassTag<V> evidence$3) {
        super(sc, conf, inputFormatClass, evidence$1, evidence$2, evidence$3);
        this.innerDeepRDD = innerDeepRDD;
    }


//    @Override
//    public T transformElement(Tuple2<K, V> tuple, Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>> config) {
//        return innerDeepRDD.transformElement(tuple, config);
//    }

}
