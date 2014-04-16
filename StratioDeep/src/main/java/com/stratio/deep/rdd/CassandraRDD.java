/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.rdd;

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
import org.apache.cassandra.utils.Pair;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaIterator;

/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a {@link com.stratio.deep.entity.Cells} element.
 */
public abstract class CassandraRDD<T> extends RDD<T> {

    private static final long serialVersionUID = -7338324965474684418L;

    /*
     * RDD configuration. This config is broadcasted to all the Sparks machines.
     */
    protected final Broadcast<IDeepJobConfig<T>> config;

    /**
     * Transform a row coming from the Cassandra's API to an element of
     * type <T>.
     *
     * @param elem
     * @return
     */
    protected abstract T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem);

    /**
     * Helper callback class called by Spark when the current RDD is computed
     * successfully. This class simply closes the {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader}
     * passed as an argument.
     *
     * @param <R>
     * @author Luca Rosellini <luca@strat.io>
     */
    class OnComputedRDDCallback<R> extends AbstractFunction0<R> {
        private final DeepRecordReader recordReader;
        private final DeepPartition deepPartition;

        public OnComputedRDDCallback(
                DeepRecordReader recordReader,
                DeepPartition dp) {
            super();
            this.recordReader = recordReader;
            this.deepPartition = dp;
        }

        @Override
        public R apply() {
            //log().debug("Closing context for partition " + deepPartition);

            recordReader.close();

            return null;
        }

    }

    /**
     * Persists the given RDD to the underlying Cassandra datastore using the java cql3 driver.<br/>
     * Beware: this method does not perform a distributed write as {@link  #saveRDDToCassandra(org.apache.spark.rdd.RDD, com.stratio.deep.config.IDeepJobConfig)}
     * does, uses the Datastax Java Driver to perform a batch write to the Cassandra server.<br/>
     * This currently scans the partitions one by one, so it will be slow if a lot of partitions are required.
     *
     * @param rdd
     * @param writeConfig
     */
    public static <W, T extends IDeepType> void cql3SaveRDDToCassandra(RDD<W> rdd, IDeepJobConfig<W> writeConfig) {
        if (IDeepType.class.isAssignableFrom(writeConfig.getEntityClass())) {
            IDeepJobConfig<T> c = (IDeepJobConfig<T>) writeConfig;
            RDD<T> r = (RDD<T>) rdd;

            CassandraRDDUtils.doCql3SaveToCassandra(r, c, new DeepType2TupleFunction<T>());
        } else if (Cells.class.isAssignableFrom(writeConfig.getEntityClass())) {
            IDeepJobConfig<Cells> c = (IDeepJobConfig<Cells>) writeConfig;
            RDD<Cells> r = (RDD<Cells>) rdd;

            CassandraRDDUtils.doCql3SaveToCassandra(r, c, new CellList2TupleFunction());
        } else {
            throw new IllegalArgumentException("Provided RDD must be an RDD of Cells or an RDD of IDeepType");
        }
    }

    /**
     * Persists the given RDD of Cells to the underlying Cassandra datastore, using configuration
     * options provided by <i>writeConfig</i>.
     *
     * @param rdd
     * @param writeConfig
     */
    public static <W, T extends IDeepType> void saveRDDToCassandra(RDD<W> rdd, IDeepJobConfig<W> writeConfig) {
        if (IDeepType.class.isAssignableFrom(writeConfig.getEntityClass())) {
            IDeepJobConfig<T> c = (IDeepJobConfig<T>) writeConfig;
            RDD<T> r = (RDD<T>) rdd;

            CassandraRDDUtils.doSaveToCassandra(r, c, new DeepType2TupleFunction<T>());
        } else if (Cells.class.isAssignableFrom(writeConfig.getEntityClass())) {
            IDeepJobConfig<Cells> c = (IDeepJobConfig<Cells>) writeConfig;
            RDD<Cells> r = (RDD<Cells>) rdd;

            CassandraRDDUtils.doSaveToCassandra(r, c, new CellList2TupleFunction());
        } else {
            throw new IllegalArgumentException("Provided RDD must be an RDD of Cells or an RDD of IDeepType");
        }
    }

    /**
     * Persists the given JavaRDD to the underlying Cassandra datastore.
     *
     * @param rdd
     * @param writeConfig
     * @param <W>
     */
    public static <W> void saveRDDToCassandra(JavaRDD<W> rdd, IDeepJobConfig<W> writeConfig) {
        saveRDDToCassandra(rdd.rdd(), writeConfig);
    }


    /**
     * Public constructor that builds a new Cassandra RDD given the context and the configuration file.
     *
     * @param sc
     * @param config
     */
    @SuppressWarnings("unchecked")
    public CassandraRDD(SparkContext sc, IDeepJobConfig<T> config) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config.getEntityClass()));
        this.config = sc.broadcast(config);
    }

    /**
     * Computes the current RDD over the given data partition. Returns an
     * iterator of Scala tuples.
     */
    @Override
    public Iterator<T> compute(Partition split, TaskContext ctx) {

        DeepPartition deepPartition = (DeepPartition) split;

        log().debug("Executing compute for split: " + deepPartition);

        final DeepRecordReader recordReader = initRecordReader(ctx, deepPartition);

        /**
         * Creates a new anonymous iterator inner class and returns it as a
         * scala iterator.
         */
        java.util.Iterator<T> recordReaderIterator = new java.util.Iterator<T>() {

            @Override
            public boolean hasNext() {
                return recordReader.hasNext();
            }

            @Override
            public T next() {
                return transformElement(recordReader.next());
            }

            @Override
            public void remove() {
                throw new DeepIOException("Method not implemented (and won't be implemented anytime soon!!!)");
            }
        };

        return new InterruptibleIterator<T>(ctx, asScalaIterator(recordReaderIterator));
    }

    /**
     * Gets an instance of the callback that will be used on the completion of the computation of this RDD.
     *
     * @param recordReader
     * @param dp
     * @return
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
     * The number of splits, and hence the number of partitions equals to the
     * number of tokens configured in cassandra.yaml + 1.
     */
    @Override
    public Partition[] getPartitions() {

        List<DeepTokenRange> underlyingInputSplits = RangeUtils.getSplits(config.value());

        Partition[] partitions = new DeepPartition[underlyingInputSplits.size()];

        for (int i = 0; i < underlyingInputSplits.size(); i++) {
            DeepTokenRange split = underlyingInputSplits.get(i);
            partitions[i] = new DeepPartition(id(), i, split);

            log().debug("Detected partition: " + partitions[i]);
        }

        return partitions;
    }

    /**
     * Returns a list of hosts on which the given split resides.
     */
    @Override
    public Seq<String> getPreferredLocations(Partition split) {
        DeepPartition p = (DeepPartition) split;

        String[] locations = p.splitWrapper().getReplicas();
        log().debug("getPreferredLocations: " + p);

        return asScalaBuffer(Arrays.asList(locations));
    }

    /**
     * Initializes a {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader} using Cassandra's Hadoop API.
     * <p/>
     * 1. Constructs a {@link org.apache.hadoop.mapreduce.TaskAttemptID}
     * 2. Constructs a {@link org.apache.hadoop.mapreduce.TaskAttemptContext} using the newly constructed
     * {@link org.apache.hadoop.mapreduce.TaskAttemptID} and the hadoop configuration contained
     * inside this RDD configuration object.
     * 3. Creates a new {@link com.stratio.deep.cql.DeepRecordReader}.
     * 4. Initialized the newly created {@link com.stratio.deep.cql.DeepRecordReader}.
     *
     * @param ctx
     * @param dp
     * @return
     */
    protected DeepRecordReader initRecordReader(TaskContext ctx, final DeepPartition dp) {
        DeepRecordReader recordReader = new DeepRecordReader(config.value(), dp.splitWrapper());
        ctx.addOnCompleteCallback(getComputeCallback(recordReader, dp));
        return recordReader;

    }
}
