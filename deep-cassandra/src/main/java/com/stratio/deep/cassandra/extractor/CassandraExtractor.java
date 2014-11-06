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

package com.stratio.deep.cassandra.extractor;

import static scala.collection.JavaConversions.asScalaBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partition;

import scala.Tuple2;
import scala.collection.Seq;

import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.cql.DeepCqlRecordWriter;
import com.stratio.deep.cassandra.cql.DeepRecordReader;
import com.stratio.deep.cassandra.cql.RangeUtils;
import com.stratio.deep.cassandra.thrift.ThriftRangeUtils;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import com.stratio.deep.commons.functions.QueryBuilder;
import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.rdd.IDeepRecordReader;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.commons.utils.Pair;

/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a
 * {@link com.stratio.deep.commons.entity.Cells} element.
 */
public abstract class CassandraExtractor<T, S extends BaseConfig<T>> implements IExtractor<T, S> {

    protected transient IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader;

    protected transient DeepCqlRecordWriter writer;

    protected CassandraDeepJobConfig<T> cassandraJobConfig;

    protected transient AbstractSerializableFunction transformer;

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
        if (recordReader != null) {
            recordReader.close();
        }

        if (writer != null) {
            writer.close();
        }

    }

    @Override
    public void initIterator(final Partition dp,
            S config) {
        if (config instanceof ExtractorConfig) {
            initWithExtractorConfig((ExtractorConfig) config);
        } else {
            cassandraJobConfig = (CassandraDeepJobConfig<T>) ((DeepJobConfig) config).initialize();
        }

        recordReader = initRecordReader((DeepPartition) dp, cassandraJobConfig);
    }

    private ICassandraDeepJobConfig<T> initWithExtractorConfig(ExtractorConfig<T> config) {
        int id = config.getRddId();
        cassandraJobConfig.setRddId(id);
        return (ICassandraDeepJobConfig<T>) ((DeepJobConfig) cassandraJobConfig).initialize(config);
    }

    public abstract T transformElement(
            Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem,
            CassandraDeepJobConfig<T> config);

    public abstract Class getConfigClass();

    /**
     * Returns the partitions on which this RDD depends on.
     * <p/>
     * Uses the underlying CqlPagingInputFormat in order to retrieve the splits.
     * <p/>
     * The number of splits, and hence the number of partitions equals to the number of tokens configured in
     * cassandra.yaml + 1.
     */

    @Override
    public Partition[] getPartitions(S config) {

        if (config instanceof ExtractorConfig) {
            initWithExtractorConfig((ExtractorConfig) config);
        } else {
            cassandraJobConfig = (CassandraDeepJobConfig) config;
        }

        List<DeepTokenRange> underlyingInputSplits = null;
        if (cassandraJobConfig.isBisectModeSet()) {
            underlyingInputSplits = RangeUtils.getSplits(cassandraJobConfig);
        } else {
            underlyingInputSplits = ThriftRangeUtils.build(cassandraJobConfig).getSplits();
        }
        Partition[] partitions = new DeepPartition[underlyingInputSplits.size()];

        int i = 0;

        for (DeepTokenRange split : underlyingInputSplits) {
            partitions[i] = new DeepPartition(cassandraJobConfig.getRddId(), i, split);

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
     * @param dp
     *            a spark deep partition
     * @return the deep record reader associated to the provided partition.
     */
    private IDeepRecordReader initRecordReader(final DeepPartition dp,
            CassandraDeepJobConfig<T> config) {

        IDeepRecordReader recordReader = new DeepRecordReader(config, dp.splitWrapper());

        return recordReader;

    }

    @Override
    public void initSave(S config, T first, QueryBuilder queryBuilder) {

        if (config instanceof ExtractorConfig) {
            cassandraJobConfig = (CassandraDeepJobConfig<T>) ((DeepJobConfig) cassandraJobConfig)
                    .initialize((ExtractorConfig) config);
        } else if (config instanceof CassandraDeepJobConfig) {
            cassandraJobConfig = (CassandraDeepJobConfig) config;
        }
        cassandraJobConfig
                .createOutputTableIfNeeded((Tuple2<Cells, Cells>) transformer.apply(first));

        if (queryBuilder == null) {
            writer = new DeepCqlRecordWriter(cassandraJobConfig);
        } else {
            writer = new DeepCqlRecordWriter(cassandraJobConfig, queryBuilder);
        }
    }

    @Override
    public void saveRDD(T t) {

        Tuple2<Cells, Cells> tuple = (Tuple2<Cells, Cells>) transformer.apply(t);

        writer.write(tuple._1(), tuple._2());
    }

}
