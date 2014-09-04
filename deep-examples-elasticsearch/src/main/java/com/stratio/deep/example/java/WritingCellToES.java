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

package com.stratio.deep.example.java;


import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.entity.ESCell;
import com.stratio.deep.extractor.ESCellExtractor;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Example class to write a RDD to mongoDB
 */
public final class WritingCellToES {
    private static final Logger LOG = Logger.getLogger(WritingCellToES.class);
    private static Long counts;

    private WritingCellToES() {
    }


    public static void main(String[] args) {
        doMain(args);
    }


    public static void doMain(String[] args) {
        String job = "java:writingCellToES";

        String host     = "localhost:9200";
        String database = "cell/input";
        String index    = "book";
        String type     = "test";
        String databaseOutput ="cell/output";

        //Call async the Extractor netty Server
        ExecutorService es = Executors.newFixedThreadPool(1);
        final Future future = es.submit(new Callable() {
            public Object call() throws Exception {
                ExtractorServer.main(null);
                return null;
            }
        });

        // Creating the Deep Context
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> config = new ExtractorConfig();

        Map<String, String> values = new HashMap<>();

        values.put(ExtractorConstants.DATABASE,    database);
        values.put(ExtractorConstants.HOST,        host );


        config.setExtractorImplClass(ESCellExtractor.class);
        config.setValues(values);

        // Creating the RDD
        RDD<Cells> rdd =  deepContext.createRDD(config);

        counts = rdd.count();

        LOG.info("----------------------- Num of rows: " + counts);

        JavaRDD<String> words =rdd.toJavaRDD().flatMap(new FlatMapFunction<Cells, String>() {
            @Override
            public Iterable<String> call(Cells cells) throws Exception {

                List<String> words = new ArrayList<>();
                for (Cells canto : (List<Cells>) cells.getCellByName("cantos").getCellValue()) {
                    words.addAll(Arrays.asList(((String) canto.getCellByName("text").getCellValue()).split(" ")));
                }
                return words;
            }
        });


        JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });


        JavaPairRDD<String, Integer>  wordCountReduced = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<Cells>  outputRDD =  wordCountReduced.map(new Function<Tuple2<String, Integer>, Cells>() {
            @Override
            public Cells call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Cells(ESCell.create("word", stringIntegerTuple2._1()) , ESCell.create("count", stringIntegerTuple2._2()));
            }
        });


        ExtractorConfig<Cells> outputConfigEntity = new ExtractorConfig<>();
        outputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, databaseOutput).putValue(ExtractorConstants.CREATE_ON_WRITE,"true");
        outputConfigEntity.setExtractorImplClass(ESCellExtractor.class);


        deepContext.saveRDD(outputRDD.rdd(), outputConfigEntity);


        ExtractorServer.close();
        deepContext.stop();
    }
}
