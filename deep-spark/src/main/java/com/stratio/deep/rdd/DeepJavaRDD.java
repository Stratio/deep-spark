package com.stratio.deep.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Created by rcrespo on 12/08/14.
 */
public class DeepJavaRDD<T> extends JavaRDD<T>{




    public DeepJavaRDD(DeepRDD<T> rdd) {
        super(rdd, ClassTag$.MODULE$.<T>apply(rdd.config.value().getEntityClass()));
    }




    @Override
    public ClassTag<T> classTag() {
        return ClassTag$.MODULE$.<T>apply(((DeepRDD<T>) this.rdd()).config.value().getEntityClass());
    }

}
