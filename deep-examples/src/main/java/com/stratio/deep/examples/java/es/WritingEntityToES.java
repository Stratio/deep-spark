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
import com.stratio.deep.testentity.BookEntity;
import com.stratio.deep.testentity.CantoEntity;
import com.stratio.deep.testentity.WordCount;
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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Example class to write an entity to ES
 */
public final class WritingEntityToES {
    private static final Logger LOG = Logger.getLogger(WritingEntityToES.class);
    public static List<Tuple2<String, Integer>> results;
    private static Long counts;
    private WritingEntityToES() {
    }

    /**
     * Application entry point.
     *
     * @param args the arguments passed to the application.
     */
    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:writingEntityToES";
        String host     = "localhost:9200";
        String database = "book/word";
        String index    = "book";
        String type     = "test";
        String database2 = "entity/output";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<BookEntity> config = new ExtractorConfig(BookEntity.class);

        Map<String, Serializable> values = new HashMap<>();

        values.put(ExtractorConstants.DATABASE,    database);
        values.put(ExtractorConstants.HOST,        host );

        config.setExtractorImplClass(ESEntityExtractor.class);
        config.setEntityClass(BookEntity.class);

        config.setValues(values);

        // Creating the RDD
        RDD<BookEntity> rdd =  deepContext.createRDD(config);


        counts = rdd.count();
        BookEntity[] collection = ( BookEntity[])rdd.cache().collect();
        LOG.info("-------------------------   Num of rows: " + counts +" ------------------------------");
        LOG.info("-------------------------   Num of Columns: " + collection.length+" ------------------------------");
        LOG.info("-------------------------   Element Canto: " + collection[0].getCantoEntities()+" ------------------------------");


        JavaRDD<String> words =rdd.toJavaRDD().flatMap(new FlatMapFunction<BookEntity, String>() {
            @Override
            public Iterable<String> call(BookEntity bookEntity) throws Exception {

                List<String> words = new ArrayList<>();
                for (CantoEntity canto : bookEntity.getCantoEntities()){
                    words.addAll(Arrays.asList(canto.getText().split(" ")));
                }
                return words;
            }
        });


        JavaPairRDD<String, Long> wordCount = words.mapToPair(new PairFunction<String, String, Long>() {
            Long req = 1L;
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                return new Tuple2<String, Long>(s,req);
            }
        });


        JavaPairRDD<String, Long>  wordCountReduced = wordCount.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long integer, Long integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<WordCount>  outputRDD =  wordCountReduced.map(new Function<Tuple2<String, Long>, WordCount>() {
            @Override
            public WordCount call(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
            }
        });


        ExtractorConfig<WordCount> outputConfigEntity = new ExtractorConfig(WordCount.class);
        outputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database2);
        outputConfigEntity.setExtractorImplClass(ESEntityExtractor.class);

        deepContext.saveRDD(outputRDD.rdd(),outputConfigEntity);



        deepContext.stop();
    }


}
