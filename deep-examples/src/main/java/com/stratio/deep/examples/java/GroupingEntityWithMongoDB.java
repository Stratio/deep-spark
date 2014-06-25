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

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.testentity.BookEntity;
import com.stratio.deep.testentity.CantoEntity;
import com.stratio.deep.testentity.WordCount;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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


        IMongoDeepJobConfig<BookEntity> inputConfigEntity =
                DeepJobConfigFactory.createMongoDB(BookEntity.class).host(host).database(database).collection(inputCollection).initialize();

        MongoJavaRDD<BookEntity> inputRDDEntity = deepContext.mongoJavaRDD(inputConfigEntity);



        JavaRDD<String> words =inputRDDEntity.flatMap(new FlatMapFunction<BookEntity, String>() {
            @Override
            public Iterable<String> call(BookEntity bookEntity) throws Exception {

                List<String> words = new ArrayList<>();
                for (CantoEntity canto : bookEntity.getCantoEntities()){
                    words.addAll(Arrays.asList(canto.getText().split(" ")));
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

        JavaRDD<WordCount>  outputRDD =  wordCountReduced.map(new Function<Tuple2<String, Integer>, WordCount>() {
            @Override
            public WordCount call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
            }
        });


        IMongoDeepJobConfig<WordCount> outputConfigEntity =
                DeepJobConfigFactory.createMongoDB(WordCount.class).host(host).database(database).collection(outputCollection).initialize();

        MongoEntityRDD.saveEntity(outputRDD,outputConfigEntity);


        deepContext.stop();
    }
}
