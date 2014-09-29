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

package com.stratio.deep.commons.extractor.impl;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.rdd.NewHadoopPartition;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.config.IDeepJobConfig;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.commons.utils.Constants;
import com.stratio.deep.commons.utils.DeepSparkHadoopMapReduceUtil;

import scala.Tuple2;

/**
 * Created by rcrespo on 26/08/14.
 */
public abstract class GenericHadoopExtractor<T, K, V, KOut, VOut> implements IExtractor<T> {

    protected IDeepJobConfig<T, ? extends IDeepJobConfig> deepJobConfig;

    protected transient RecordReader<K, V> reader;

    protected transient RecordWriter<KOut, VOut> writer;

    protected transient InputFormat<K, V> inputFormat;

    protected transient OutputFormat<KOut, VOut> outputFormat;

    protected transient String jobTrackerId;

    protected transient TaskAttemptContext hadoopAttemptContext;

    protected boolean havePair = false;

    protected boolean finished = false;

    protected transient JobID jobId = null;

    private static final Logger LOG = Logger.getLogger(GenericHadoopExtractor.class);

    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        jobTrackerId = formatter.format(new Date());

    }

    @Override
    public Partition[] getPartitions(ExtractorConfig<T> config) {

        addSparkIdToDeepJobConfig(config);

        return getPartitions(deepJobConfig);
    }

    @Override
    public Partition[] getPartitions(IDeepJobConfig<T, ?> config) {

        int id = Integer.parseInt(config.getCustomConfiguration().get(Constants.SPARK_RDD_ID).toString());
        jobId = new JobID(jobTrackerId, id);

        deepJobConfig = (IDeepJobConfig<T, ? extends IDeepJobConfig>) config.initialize();
        Configuration conf = deepJobConfig.getHadoopConfiguration();

        JobContext jobContext = DeepSparkHadoopMapReduceUtil.newJobContext(conf, jobId);

        try {
            List<InputSplit> splits = inputFormat.getSplits(jobContext);

            Partition[] partitions = new Partition[(splits.size())];
            for (int i = 0; i < splits.size(); i++) {
                partitions[i] = new NewHadoopPartition(id, i, splits.get(i));
            }

            return partitions;

        } catch (IOException | InterruptedException | RuntimeException e) {
            LOG.error("Impossible to calculate partitions " + e.getMessage());
            throw new DeepGenericException("Impossible to calculate partitions " + e.getMessage());
        }

    }

    @Override
    public boolean hasNext() {
        if (!finished && !havePair) {
            try {
                finished = !reader.nextKeyValue();
            } catch (IOException | InterruptedException e) {
                LOG.error("Impossible to get hasNext " + e.getMessage());
                throw new DeepGenericException("Impossible to get hasNext " + e.getMessage());
            }
            havePair = !finished;

        }
        return !finished;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("End of stream");
        }
        havePair = false;

        Tuple2<K, V> tuple = null;
        try {
            return transformElement(new Tuple2<>((K) reader.getCurrentKey(), (V) reader.getCurrentValue()),
                    deepJobConfig);
        } catch (IOException | InterruptedException e) {
            LOG.error("Impossible to get next value " + e.getMessage());
            throw new DeepGenericException("Impossible to get next value " + e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close(hadoopAttemptContext);
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("Impossible to close RecordReader " + e.getMessage());
            throw new DeepGenericException("Impossible to close RecordReader " + e.getMessage());
        }
    }

    @Override
    public void initIterator(Partition dp, ExtractorConfig<T> config) {

        addSparkIdToDeepJobConfig(config);
        initIterator(dp, deepJobConfig);

    }

    private void addSparkIdToDeepJobConfig(ExtractorConfig<T> config) {
        int id = Integer.parseInt(config.getValues().get(Constants.SPARK_RDD_ID).toString());

        //the new map
        Map<String, Serializable> serializableMap = new HashMap<>();
        serializableMap.put(Constants.SPARK_RDD_ID, id);

        //customConfiguration
        Map<String, Serializable> serializableMapCustom = deepJobConfig.getCustomConfiguration();
        serializableMapCustom.putAll(serializableMap);

        deepJobConfig = (IDeepJobConfig<T, ? extends IDeepJobConfig>) deepJobConfig.initialize(config);
        deepJobConfig.customConfiguration(serializableMapCustom);

    }

    public abstract T transformElement(Tuple2<K, V> tuple, IDeepJobConfig<T, ? extends IDeepJobConfig> config);

    @Override
    public IExtractor getExtractorInstance(ExtractorConfig<T> config) {
        return this;
    }

    @Override
    public void saveRDD(T t) {
        Tuple2<KOut, VOut> tuple = transformElement(t);
        try {
            writer.write(tuple._1(), tuple._2());

        } catch (IOException | InterruptedException e) {
            LOG.error("Impossible to saveRDD " + e.getMessage());
            throw new DeepGenericException("Impossible to saveRDD " + e.getMessage());
        }
        return;
    }

    @Override
    public void initSave(ExtractorConfig<T> config, T first) {
        int id = Integer.parseInt(config.getValues().get(Constants.SPARK_RDD_ID).toString());

        int partitionIndex = Integer.parseInt(config.getValues().get(Constants.SPARK_PARTITION_ID).toString());

        TaskAttemptID attemptId = DeepSparkHadoopMapReduceUtil
                .newTaskAttemptID(jobTrackerId, id, true, partitionIndex, 0);

        hadoopAttemptContext = DeepSparkHadoopMapReduceUtil
                .newTaskAttemptContext(deepJobConfig.initialize(config).getHadoopConfiguration(), attemptId);
        try {
            writer = outputFormat.getRecordWriter(hadoopAttemptContext);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initIterator(Partition dp, IDeepJobConfig<T, ?> deepJobConfig) {

        int id = Integer.parseInt(deepJobConfig.getCustomConfiguration().get(Constants.SPARK_RDD_ID).toString());

        NewHadoopPartition split = (NewHadoopPartition) dp;

        TaskAttemptID attemptId = DeepSparkHadoopMapReduceUtil
                .newTaskAttemptID(jobTrackerId, id, true, split.index(), 0);

        TaskAttemptContext hadoopAttemptContext = DeepSparkHadoopMapReduceUtil
                .newTaskAttemptContext(deepJobConfig.getHadoopConfiguration(), attemptId);

        try {
            reader = inputFormat.createRecordReader(split.serializableHadoopSplit().value(), hadoopAttemptContext);
            reader.initialize(split.serializableHadoopSplit().value(), hadoopAttemptContext);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public abstract Tuple2<KOut, VOut> transformElement(T record);
}
