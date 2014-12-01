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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.CantoEntity;
import com.stratio.deep.core.entity.WordCount;
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties;
import com.stratio.deep.mongodb.config.MongoConfigFactory;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;

import scala.Tuple2;

/**
 * Created by rcrespo on 25/06/14.
 */
public final class GroupingEntityWithMongoDB {

    private GroupingEntityWithMongoDB() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "scala:groupingEntityWithMongoDB";

        String host = "localhost:27017";

        String database = "book";
        String inputCollection = "input";
        String outputCollection = "output";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        MongoDeepJobConfig<BookEntity> inputConfigEntity =
                MongoConfigFactory.createMongoDB(BookEntity.class).host(host).database(database)
                        .collection(inputCollection).initialize();

        JavaRDD<BookEntity> inputRDDEntity = deepContext.createJavaRDD(inputConfigEntity);
        JavaRDD<String> words = inputRDDEntity.flatMap(new FlatMapFunction<BookEntity, String>() {
            @Override
            public Iterable<String> call(BookEntity bookEntity) throws Exception {

                List<String> words = new ArrayList<>();
                for (CantoEntity canto : bookEntity.getCantoEntities()) {
                    words.addAll(Arrays.asList(canto.getText().split(" ")));
                }
                return words;
            }
        });

        JavaPairRDD<String, Long> wordCount = words.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                return new Tuple2<>(s, 1l);
            }
        });

        JavaPairRDD<String, Long> wordCountReduced = wordCount.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long integer, Long integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<WordCount> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Long>, WordCount>() {
            @Override
            public WordCount call(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
            }
        });

        MongoDeepJobConfig<WordCount> outputConfigEntity =
                MongoConfigFactory.createMongoDB(WordCount.class).host(host).database(database)
                        .collection(outputCollection).initialize();

        deepContext.saveRDD(outputRDD.rdd(), outputConfigEntity);

        deepContext.stop();
    }
}
