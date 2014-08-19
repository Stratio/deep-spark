package com.stratio.deep.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.DeepHadoopRDD;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.HadoopRDD$;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

/**
 * Created by rcrespo on 12/08/14.
 */
public class DeepGenericHadoopRDD<T, K,V> extends DeepHadoopRDD<T,K,V> {


    private IDeepHadoopRDD<T,K, V> innerDeepRDD;

    public DeepGenericHadoopRDD(SparkContext sc, IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> conf, Class<? extends org.apache.hadoop.mapreduce.InputFormat<K, V>> inputFormatClass, IDeepHadoopRDD<T, K, V> innerDeepRDD, ClassTag<T> evidence$1, ClassTag<K> evidence$2, ClassTag<V> evidence$3) {
        super(sc, conf, inputFormatClass, evidence$1, evidence$2, evidence$3);
        this.innerDeepRDD = innerDeepRDD;
    }

    @Override
    public T transformElement(Tuple2<K, V> tuple, Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?,?>>>  config) {
        return innerDeepRDD.transformElement(tuple, config);
    }
}
