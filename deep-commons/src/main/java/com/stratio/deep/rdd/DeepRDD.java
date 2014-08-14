package com.stratio.deep.rdd;

import com.stratio.deep.exception.DeepInstantiationException;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.extractor.client.ExtractorClient;

import javax.net.ssl.SSLException;

/**
 * Created by rcrespo on 11/08/14.
 */
public class DeepRDD<T> extends RDD<T> {

  private static final long serialVersionUID = -5360986039609466526L;

  private ExtractorClient<T> extractorClient = new ExtractorClient<>();

  protected Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>> config;

  public DeepRDD(SparkContext sc,
      IDeepJobConfig<T, IDeepJobConfig<T, ? extends IDeepJobConfig>> config) {
    super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
        .getEntityClass()));
    this.config =
        sc.broadcast(config, ClassTag$.MODULE$
            .<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>>apply(config.getClass()));
      try {
          extractorClient.initialize();
      } catch (SSLException | InterruptedException e) {
          throw new DeepInstantiationException("error creating IDeepRDD " +e.getMessage());
      }
  }

  public DeepRDD(SparkContext sc, Class entityClass) {
    super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(entityClass));
  }

  @Override
  public Iterator<T> compute(Partition split, TaskContext context) {
    return extractorClient.compute(split, context, config.getValue());
  }

  @Override
  public Partition[] getPartitions() {
    return extractorClient.getPartitions(config.getValue(), id());
  }

  public Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>> getConfig() {
    return config;
  }
}
