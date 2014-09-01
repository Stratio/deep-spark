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

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.extractor.server.ExtractorServer;
import com.stratio.deep.extractor.utils.ExtractorConstants;
import com.stratio.deep.rdd.CassandraCellExtractor;

import com.stratio.deep.rdd.CassandraEntityExtractor;
import com.stratio.deep.testentity.TweetEntity;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 13-feb-2014
 */
public final class AggregatingData {
    private static final Logger LOG = Logger.getLogger(AggregatingData.class);

    /* used to perform external tests */
    private static Double avg;
    private static Double variance;
    private static Double stddev;
    private static Double count;

    private AggregatingData() {
    }

    /**
     * Application entry point.
     *
     * @param args the arguments passed to the application.
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
        String job = "java:aggregatingData";

        String KEYSPACENAME = "twitter";
        String TABLENAME    = "tweets";
        String CQLPORT      = "9042";
        String RPCPORT      = "9160";
        String HOST         = "127.0.0.1";


        ExtractorServer.initExtractorServer();

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> config = new ExtractorConfig<>();

        config.setExtractorImplClass(CassandraCellExtractor.class);
//        config.setEntityClass(TweetEntity.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE, KEYSPACENAME);
        values.put(ExtractorConstants.TABLE,    TABLENAME);
        values.put(ExtractorConstants.CQLPORT,  CQLPORT);
        values.put(ExtractorConstants.RPCPORT,  RPCPORT);
        values.put(ExtractorConstants.HOST,     HOST );

        config.setValues(values);


        // Creating the RDD
        RDD<Cells> rdd = deepContext.createRDD(config);
        LOG.info("count: " + rdd.count());
        LOG.info("first: " + rdd.first());


        ExtractorServer.close();
        deepContext.stop();
    }

    public static Double getAvg() {
        return avg;
    }

    public static Double getVariance() {
        return variance;
    }

    public static Double getStddev() {
        return stddev;
    }

    public static Double getCount() {
        return count;
    }
}
