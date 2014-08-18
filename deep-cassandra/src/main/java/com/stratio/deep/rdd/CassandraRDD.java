/*
 * Copyright 2014, Stratio.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.stratio.deep.rdd;

import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaIterator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.cql.DeepRecordReader;
import com.stratio.deep.cql.DeepTokenRange;
import com.stratio.deep.cql.RangeUtils;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.functions.CellList2TupleFunction;
import com.stratio.deep.functions.DeepType2TupleFunction;
import com.stratio.deep.partition.impl.DeepPartition;
import com.stratio.deep.utils.Pair;

/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a
 * {@link com.stratio.deep.entity.Cells} element.
 */
public abstract class CassandraRDD<T> implements IDeepRDD<T> {

  /**
   * RDD configuration. This config is broadcasted to all the Sparks machines.
   */
  // protected final Broadcast<ICassandraDeepJobConfig<T>> config;



  /**
   * Helper callback class called by Spark when the current RDD is computed successfully. This class
   * simply closes the {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader} passed as an
   * argument.
   * 
   * @param <R>
   * @author Luca Rosellini <luca@strat.io>
   */
  class OnComputedRDDCallback<R> extends AbstractFunction0<R> {
    private final DeepRecordReader recordReader;
    private final DeepPartition deepPartition;

    public OnComputedRDDCallback(DeepRecordReader recordReader, DeepPartition dp) {
      super();
      this.recordReader = recordReader;
      this.deepPartition = dp;
        System.out.print("termina el cbuilder " +this);
    }

    @Override
    public R apply() {
      recordReader.close();

      return null;
    }

  }

  /**
   * Persists the given RDD to the underlying Cassandra datastore using the java cql3 driver.<br/>
   * Beware: this method does not perform a distributed write as
   * {@link com.stratio.deep.rdd.CassandraRDD#saveRDDToCassandra} does, uses the Datastax Java
   * Driver to perform a batch write to the Cassandra server.<br/>
   * This currently scans the partitions one by one, so it will be slow if a lot of partitions are
   * required.
   * 
   * @param rdd the RDD to persist.
   * @param writeConfig the write configuration object.
   */
  @SuppressWarnings("unchecked")
  public static <W, T extends IDeepType> void cql3SaveRDDToCassandra(RDD<W> rdd,
      ICassandraDeepJobConfig<W> writeConfig) {
    if (IDeepType.class.isAssignableFrom(writeConfig.getEntityClass())) {
      ICassandraDeepJobConfig<T> c = (ICassandraDeepJobConfig<T>) writeConfig;
      RDD<T> r = (RDD<T>) rdd;

      CassandraRDDUtils.doCql3SaveToCassandra(r, c, new DeepType2TupleFunction<T>());
    } else if (Cells.class.isAssignableFrom(writeConfig.getEntityClass())) {
      ICassandraDeepJobConfig<Cells> c = (ICassandraDeepJobConfig<Cells>) writeConfig;
      RDD<Cells> r = (RDD<Cells>) rdd;

      CassandraRDDUtils.doCql3SaveToCassandra(r, c, new CellList2TupleFunction());
    } else {
      throw new IllegalArgumentException(
          "Provided RDD must be an RDD of Cells or an RDD of IDeepType");
    }
  }

  /**
   * Persists the given RDD of Cells to the underlying Cassandra datastore, using configuration
   * options provided by <i>writeConfig</i>.
   * 
   * @param rdd the RDD to persist.
   * @param writeConfig the write configuration object.
   */
  @SuppressWarnings("unchecked")
  public static <W, T extends IDeepType> void saveRDDToCassandra(RDD<W> rdd,
      ICassandraDeepJobConfig<W> writeConfig) {
    if (IDeepType.class.isAssignableFrom(writeConfig.getEntityClass())) {
      ICassandraDeepJobConfig<T> c = (ICassandraDeepJobConfig<T>) writeConfig;
      RDD<T> r = (RDD<T>) rdd;

      CassandraRDDUtils.doSaveToCassandra(r, c, new DeepType2TupleFunction<T>());
    } else if (Cells.class.isAssignableFrom(writeConfig.getEntityClass())) {
      ICassandraDeepJobConfig<Cells> c = (ICassandraDeepJobConfig<Cells>) writeConfig;
      RDD<Cells> r = (RDD<Cells>) rdd;

      CassandraRDDUtils.doSaveToCassandra(r, c, new CellList2TupleFunction());
    } else {
      throw new IllegalArgumentException(
          "Provided RDD must be an RDD of Cells or an RDD of IDeepType");
    }
  }

  /**
   * Persists the given JavaRDD to the underlying Cassandra datastore.
   * 
   * @param rdd the RDD to persist.
   * @param writeConfig the write configuration object.
   * @param <W> the generic type associated to the provided configuration object.
   */
  public static <W> void saveRDDToCassandra(JavaRDD<W> rdd, ICassandraDeepJobConfig<W> writeConfig) {
    saveRDDToCassandra(rdd.rdd(), writeConfig);
  }


  // /**
  // * Public constructor that builds a new Cassandra RDD given the context and the configuration
  // file.
  // *
  // * @param sc the spark context to which the RDD will be bound to.
  // * @param config the deep configuration object.
  // */
  // @SuppressWarnings("unchecked")
  // public CassandraRDD(SparkContext sc, ICassandraDeepJobConfig<T> config) {
  // super(sc, scala.collection.Seq$.MODULE$.empty(),
  // ClassTag$.MODULE$.<T>apply(config.getEntityClass()));
  // this.config = sc.broadcast(config,
  // ClassTag$.MODULE$.<ICassandraDeepJobConfig<T>>apply(config.getClass()));
  // }

  /**
   * Computes the current RDD over the given data partition. Returns an iterator of Scala tuples.
   */
  @Override
  public Iterator<T> compute(Partition split, TaskContext ctx,
      final IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {


      ctx = new TaskContext (ctx.stageId(), ctx.partitionId(), ctx.attemptId(), ctx.runningLocally(), ctx.taskMetrics());

    DeepPartition deepPartition = (DeepPartition) split;

    // log().debug("Executing compute for split: " + deepPartition);

    final DeepRecordReader recordReader = initRecordReader(ctx, deepPartition, config);

    /**
     * Creates a new anonymous iterator inner class and returns it as a scala iterator.
     */
    java.util.Iterator<T> recordReaderIterator = new java.util.Iterator<T>() {

      @Override
      public boolean hasNext() {
        return recordReader.hasNext();
      }

      @Override
      public T next() {
        return transformElement(recordReader.next(), config);
      }

      @Override
      public void remove() {
        throw new DeepIOException(
            "Method not implemented (and won't be implemented anytime soon!!!)");
      }
    };

    return new InterruptibleIterator<T>(ctx, asScalaIterator(recordReaderIterator));
  }

  protected abstract T transformElement(
      Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem,
      IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config);

  /**
   * Gets an instance of the callback that will be used on the completion of the computation of this
   * RDD.
   * 
   * @param recordReader the deep record reader.
   * @param dp the spark deep partition.
   * @return an instance of the callback that will be used on the completion of the computation of
   *         this RDD.
   */
  protected AbstractFunction0<BoxedUnit> getComputeCallback(DeepRecordReader recordReader,
      DeepPartition dp) {
    return new OnComputedRDDCallback<>(recordReader, dp);
  }

  /**
   * Returns the partitions on which this RDD depends on.
   * <p/>
   * Uses the underlying CqlPagingInputFormat in order to retrieve the splits.
   * <p/>
   * The number of splits, and hence the number of partitions equals to the number of tokens
   * configured in cassandra.yaml + 1.
   */
  @Override
  public Partition[] getPartitions(IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config, int id) {

    List<DeepTokenRange> underlyingInputSplits = RangeUtils.getSplits(config);

    Partition[] partitions = new DeepPartition[underlyingInputSplits.size()];

    int i = 0;

    for (DeepTokenRange split : underlyingInputSplits) {
      partitions[i] = new DeepPartition(id, i, split);

      // log().debug("Detected partition: " + partitions[i]);
      ++i;
    }

    return partitions;
  }

  /**
   * Returns a list of hosts on which the given split resides.
   */
  public Seq<String> getPreferredLocations(Partition split) {
    DeepPartition p = (DeepPartition) split;

    List<String> locations = p.splitWrapper().getReplicas();
    // log().debug("getPreferredLocations: " + p);

    return asScalaBuffer(locations);
  }

  /**
   * Instantiates a new deep record reader object associated to the provided partition.
   * 
   * @param ctx the spark task context.
   * @param dp a spark deep partition
   * @return the deep record reader associated to the provided partition.
   */
  private DeepRecordReader initRecordReader(TaskContext ctx, final DeepPartition dp,
      IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {
    DeepRecordReader recordReader =
        new DeepRecordReader((ICassandraDeepJobConfig) config, dp.splitWrapper());
      ctx.addOnCompleteCallback(getComputeCallback(recordReader, dp));
    return recordReader;

  }
}
