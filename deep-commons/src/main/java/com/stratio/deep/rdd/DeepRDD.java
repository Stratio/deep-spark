package com.stratio.deep.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

/**
 * Created by rcrespo on 11/08/14.
 */
public class DeepRDD<T> extends RDD<T> {


    private IDeepRDD<T> innerDeepRDD;

    protected final Broadcast<IDeepJobConfig<T, IDeepJobConfig<T, ? extends IDeepJobConfig>>> config;

    public DeepRDD (SparkContext sc, IDeepJobConfig<T, IDeepJobConfig<T, ? extends IDeepJobConfig>> config, IDeepRDD<T> innerDeepRDD){
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config.getEntityClass()));
        this.config = sc.broadcast(config, ClassTag$.MODULE$.<IDeepJobConfig<T, IDeepJobConfig<T, ? extends IDeepJobConfig>>>apply(config.getClass()));
        this.innerDeepRDD = innerDeepRDD;
    }
    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {
        return innerDeepRDD.compute(split, context, config);
    }

    @Override
    public Partition[] getPartitions() {
        return innerDeepRDD.getPartitions(config, id());
    }
}
