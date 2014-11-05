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

package com.stratio.deep.examples.java;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.rdd.DeepJavaRDD;
import com.stratio.deep.utils.ContextProperties;

/**
 * Author: Emmanuelle Raffenne Date..: 13-feb-2014
 */
public class SaveMaxExamples {
    private static final Logger LOG = Logger.getLogger(SaveMaxExamples.class);

    /* used to perform external tests */
    private static Double avg;
    private static Double variance;
    private static Double stddev;
    private static Double count;

    private SaveMaxExamples() {
    }

    /**
     * Application entry point.
     *
     * @param args
     *            the arguments passed to the application.
     */
    public static void main(String[] args) {
        doMain(args);
    }

    /**
     * This is the method called by both main and tests.
     *
     * @param args
     */
    public static void doMain(String[] args) {
        String job = "java:saveMax";

        // Creating the Deep Context
        ContextProperties p = new ContextProperties(args);
        SparkConf sparkConf = new SparkConf().setMaster(p.getCluster()).setAppName(job).setJars(p.getJars())
                        .setSparkHome(p.getSparkHome());
        // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // .set("spark.kryo.registrator", "com.stratio.deep.serializer.DeepKryoRegistrator");

        SparkContext sc = new SparkContext(p.getCluster(), job, sparkConf);

        LOG.info("spark.serializer: " + System.getProperty("spark.serializer"));
        LOG.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        DeepSparkContext deepContext = new DeepSparkContext(sc);

        // Configuration and initialization
        ExtractorConfig<Cells> config = new ExtractorConfig<>(Cells.class);
        config.setExtractorImplClass(CassandraCellExtractor.class);

        String keySpace = "test";
        String tableName = "tweets";
        String cqlPort = "9042";
        String rpcPort = "9160";
        String host = "127.0.0.1";
        Map<String, Serializable> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE, keySpace);
        values.put(ExtractorConstants.TABLE, tableName);
        values.put(ExtractorConstants.CQLPORT, cqlPort);
        values.put(ExtractorConstants.RPCPORT, rpcPort);
        values.put(ExtractorConstants.HOST, host);

        config.setValues(values);

        // Creating the RDD
        DeepJavaRDD deepRdd = (DeepJavaRDD) deepContext.createJavaRDD(config);
        // deepRdd.saveMax("favorite_count");

        deepRdd.rdd().log().info("*******************************Num of rows: " + deepRdd.count());

        // **********************************************/*

        String KEYSPACENAME = "test";
        String TABLENAME = "tweets";
        String HOST = "127.0.0.1";
        final String outputTableName = "counters";

        // --- OUTPUT RDD
        ExtractorConfig<Cells> outputConfig = new ExtractorConfig();

        outputConfig.setExtractorImplClass(CassandraCellExtractor.class);
        Map<String, Serializable> valuesOutput = new HashMap<>();
        valuesOutput.put(ExtractorConstants.KEYSPACE, KEYSPACENAME);
        valuesOutput.put(ExtractorConstants.TABLE, outputTableName);
        valuesOutput.put(ExtractorConstants.CQLPORT, cqlPort);
        valuesOutput.put(ExtractorConstants.RPCPORT, rpcPort);
        valuesOutput.put(ExtractorConstants.HOST, HOST);
        valuesOutput.put(ExtractorConstants.CREATE_ON_WRITE, true);

        outputConfig.setValues(valuesOutput); 

        deepContext.saveMaxRDD(deepRdd.rdd(), outputConfig, "favorite_count", "tweet_id");

        deepContext.stop();
    }
}
