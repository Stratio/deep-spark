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

package com.stratio.deep.examples.java.es;


import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.extractor.ESEntityExtractor;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.testentity.WordCount;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Example class to read an entity from mongoDB
 */
public final class ReadingEntityFromES {

    private static final Logger LOG = Logger.getLogger(ReadingEntityFromES.class);
    public static List<Tuple2<String, Integer>> results;
    private static Long counts;
    private ReadingEntityFromES() {
    }


    public static void main(String[] args) {
        doMain(args);
    }


    public static void doMain(String[] args) {
        String job      = "java:readingEntityWithES";
        String host     = "localhost:9200";
        String database = "entity/output";
        String index    = "book";
        String type     = "test";
        //Call async the Extractor netty Server
        ExecutorService es = Executors.newFixedThreadPool(1);
        final Future future = es.submit(new Callable() {
            public Object call() throws Exception {
                ExtractorServer.main(null);
                return null;
            }
        });

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<WordCount> config = new ExtractorConfig(WordCount.class);

        Map<String, Serializable> values = new HashMap<>();

        values.put(ExtractorConstants.DATABASE,    database);
        values.put(ExtractorConstants.HOST,        host );

        config.setExtractorImplClass(ESEntityExtractor.class);
        config.setEntityClass(WordCount.class);

        config.setValues(values);

        // Creating the RDD
        RDD<WordCount> rdd =  deepContext.createRDD(config);

        counts = rdd.count();
        WordCount[] collection = ( WordCount[])rdd.collect();
        LOG.info("-------------------------   Num of rows: " + counts +" ------------------------------");
        LOG.info("-------------------------   Num of Columns: " + collection.length+" ------------------------------");
        LOG.info("-------------------------   Element Canto: " + collection[0].getWord()+" ------------------------------");
        ExtractorServer.close();
        deepContext.stop();


    }
}
