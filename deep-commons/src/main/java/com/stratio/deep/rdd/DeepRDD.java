package com.stratio.deep.rdd;

import java.io.Serializable;

import javax.net.ssl.SSLException;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepInstantiationException;
import com.stratio.deep.extractor.client.ExtractorClient;

/**
 * Created by rcrespo on 11/08/14.
 */
public class DeepRDD<T> extends RDD<T> implements Serializable {

  private static final long serialVersionUID = -5360986039609466526L;

  private transient ExtractorClient<T> extractorClient;

  protected Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>> config;

  public DeepRDD(SparkContext sc,
      IDeepJobConfig<T, IDeepJobConfig<T, ? extends IDeepJobConfig>> config) {
    super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
        .getEntityClass()));
    this.config =
        sc.broadcast(config, ClassTag$.MODULE$
            .<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>>apply(config.getClass()));
  }

  public DeepRDD(SparkContext sc, Class entityClass) {
    super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(entityClass));
  }

  @Override
  public Iterator<T> compute(Partition split, TaskContext context) {
    
    if (extractorClient == null) {
      extractorClient = new ExtractorClient<>();
      try {
        extractorClient.initialize();
      } catch (SSLException | InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    return extractorClient.compute(split, context, config.getValue());
  }

  @Override
  public Partition[] getPartitions() {
    
    if (extractorClient == null) {
      extractorClient = new ExtractorClient<>();
      try {
        extractorClient.initialize();
      } catch (SSLException | InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    return extractorClient.getPartitions(config.getValue(), id());
  }

  public Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>>> getConfig() {
    return config;
  }
}
