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

import static com.stratio.deep.utils.Constants.SPARK_RDD_ID;
import static scala.collection.JavaConversions.asScalaBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partition;

import scala.Tuple2;
import scala.collection.Seq;

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.config.GenericDeepJobConfig;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.cql.DeepCqlRecordWriter;
import com.stratio.deep.cql.DeepRecordReader;
import com.stratio.deep.cql.RangeUtils;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.functions.AbstractSerializableFunction;
import com.stratio.deep.partition.impl.DeepPartition;
import com.stratio.deep.utils.Pair;


/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a
 * {@link com.stratio.deep.entity.Cells} element.
 */
public abstract class CassandraExtractor<T> implements IExtractor<T> {


    /**
	 * 
	 */
	private static final long serialVersionUID = 5695449574672303372L;

	protected transient IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader;

    protected transient DeepCqlRecordWriter writer;

    protected ICassandraDeepJobConfig<T> cassandraJobConfig;

    protected transient AbstractSerializableFunction transformer;



    @Override
    public synchronized boolean hasNext() {
        return recordReader.hasNext();
    }

    @Override
    public synchronized T next() {
        return transformElement(recordReader.next(), cassandraJobConfig);
    }

    @Override
    public void close() {
        if(recordReader!=null){
            recordReader.close();
        }

        if(writer!=null){
            writer.close();
        }

    }


    @Override
    public void initIterator(final Partition dp,
                             ExtractorConfig<T> config) {
        this.cassandraJobConfig = initCustomConfig(config);
    recordReader = initRecordReader((DeepPartition) dp, cassandraJobConfig);
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

    int id = Integer.parseInt(config.getValues().get(SPARK_RDD_ID));
        ICassandraDeepJobConfig<T> cellDeepJobConfig = initCustomConfig(config);

        List<DeepTokenRange> underlyingInputSplits = RangeUtils.getSplits(cellDeepJobConfig);

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
  private IDeepRecordReader initRecordReader(final DeepPartition dp,
                                               IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {

    IDeepRecordReader recordReader = new DeepRecordReader(config, dp.splitWrapper());

        return recordReader;

    }


    @Override
    public void initSave(ExtractorConfig<T> config, T first){

        cassandraJobConfig = cassandraJobConfig.initialize(config);
        ((GenericDeepJobConfig)cassandraJobConfig).createOutputTableIfNeeded( (Tuple2<Cells,Cells> )transformer.apply(first));
        writer = new DeepCqlRecordWriter(cassandraJobConfig);
    }

    @Override
    public void saveRDD(T t) {
        Tuple2<Cells,Cells> tuple= (Tuple2<Cells,Cells> )transformer.apply(t);
        writer.write(tuple._1(), tuple._2());
    }



}
