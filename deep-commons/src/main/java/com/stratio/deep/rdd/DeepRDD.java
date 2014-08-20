package com.stratio.deep.rdd;


import static scala.collection.JavaConversions.asScalaIterator;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.net.ssl.SSLException;

import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.utils.Pair;
import org.apache.spark.InterruptibleIterator;
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


    protected Broadcast<DeepJobConfig<T>> config;

  public DeepRDD(SparkContext sc,DeepJobConfig<T> config) {
    super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
        .getEntityClass()));
    this.config =
        sc.broadcast(config, ClassTag$.MODULE$
            .<DeepJobConfig<T>>apply(config.getClass()));

      try {
          ExtractorClient<T> extractor = new ExtractorClient<>();
          extractor.initialize();
//          this.extractorClient =sc.broadcast(extractor, ClassTag$.MODULE$.<ExtractorClient<T>>apply(extractor.getClass()));
      } catch (SSLException e) {
          e.printStackTrace();
      } catch (InterruptedException e) {
          e.printStackTrace();
      }
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
      extractorClient.initIterator((IDeepPartition)split, config.getValue());
      java.util.Iterator<T> iterator = new DeepIterator<T>() {

          @Override
          public boolean hasNext() {
              return extractorClient.hasNext();
          }

          @Override
          public T next() {
              return extractorClient.next();
          }

          @Override
          public void remove() {
              throw new DeepIOException(
                      "Method not implemented (and won't be implemented anytime soon!!!)");
          }
      };



      return new InterruptibleIterator<>(context , asScalaIterator(iterator)) ;

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
//    return extractorClient.value().getPartitions(config.getValue(), id());
  }

  public Broadcast<DeepJobConfig<T>> getConfig() {
    return config;
  }


}
