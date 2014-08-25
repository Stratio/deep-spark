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

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.cql.DeepRecordReader;
import com.stratio.deep.cql.RangeUtils;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.functions.CellList2TupleFunction;
import com.stratio.deep.functions.DeepType2TupleFunction;
import com.stratio.deep.utils.Pair;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.collection.Seq;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static scala.collection.JavaConversions.asScalaBuffer;


/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a
 * {@link com.stratio.deep.entity.Cells} element.
 */
public abstract class CassandraExtractor<T> implements IExtractor<T> {


    private IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader;



    protected ICassandraDeepJobConfig<T> cassandraJobConfig;

    /**
     * Persists the given RDD to the underlying Cassandra datastore using the java cql3 driver.<br/>
     * Beware: this method does not perform a distributed write as
     * {@link CassandraExtractor#saveRDDToCassandra} does, uses the Datastax Java
     * Driver to perform a batch write to the Cassandra server.<br/>
     * This currently scans the partitions one by one, so it will be slow if a lot of partitions are
     * required.
     *
     * @param rdd         the RDD to persist.
     * @param writeConfig the write configuration object.
     */
    @SuppressWarnings("unchecked")
    public static <W, T extends IDeepType> void cql3SaveRDDToCassandra(RDD<W> rdd,
                                                                       ExtractorConfig<W> writeConfig) {
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
     * @param rdd         the RDD to persist.
     * @param writeConfig the write configuration object.
     */
    @SuppressWarnings("unchecked")
    public static <W, T extends IDeepType> void saveRDDToCassandra(RDD<W> rdd,
                                                                   ExtractorConfig<W> writeConfig) {
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
     * @param rdd         the RDD to persist.
     * @param writeConfig the write configuration object.
     * @param <W>         the generic type associated to the provided configuration object.
     */
    public static <W> void saveRDDToCassandra(JavaRDD<W> rdd, ExtractorConfig<W> writeConfig) {
        saveRDDToCassandra(rdd.rdd(), writeConfig);
    }


    @Override
    public boolean hasNext() {
        return recordReader.hasNext();
    }

    @Override
    public T next() {
        return transformElement(recordReader.next(), cassandraJobConfig);
    }

    @Override
    public void close() {
        recordReader.close();
    }


    @Override
    public void initIterator(final Partition dp,
                             ExtractorConfig<T> config) {
        this.cassandraJobConfig = initCustomConfig(config);
        recordReader = initRecordReader((DeepTokenRange) dp, cassandraJobConfig);
    }



    private ICassandraDeepJobConfig<T> initCustomConfig(ExtractorConfig<T> config){
        return cassandraJobConfig.initialize(config);
    }

    public abstract T transformElement(
            Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem,
            IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config);

    public abstract Class getConfigClass();

    /**
     * Returns the partitions on which this RDD depends on.
     * <p/>
     * Uses the underlying CqlPagingInputFormat in order to retrieve the splits.
     * <p/>
     * The number of splits, and hence the number of partitions equals to the number of tokens
     * configured in cassandra.yaml + 1.
     */
    @Override
    public Partition[] getPartitions(ExtractorConfig<T> config) {


        ICassandraDeepJobConfig<T> cellDeepJobConfig = initCustomConfig(config);

        List<DeepTokenRange> underlyingInputSplits = RangeUtils.getSplits(cellDeepJobConfig);

        return underlyingInputSplits.toArray(new Partition[underlyingInputSplits.size()]);
    }

    /**
     * Returns a list of hosts on which the given split resides.
     */
    public Seq<String> getPreferredLocations(DeepTokenRange tokenRange) {

        List<String> locations = tokenRange.getReplicas();
        // log().debug("getPreferredLocations: " + p);

        return asScalaBuffer(locations);
    }


    /**
     * Instantiates a new deep record reader object associated to the provided partition.
     *
     * @param dp a spark deep partition
     * @return the deep record reader associated to the provided partition.
     */
    private IDeepRecordReader initRecordReader(final DeepTokenRange dp,
                                               IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {

        IDeepRecordReader recordReader = new DeepRecordReader(config, dp);

        return recordReader;

    }


}
