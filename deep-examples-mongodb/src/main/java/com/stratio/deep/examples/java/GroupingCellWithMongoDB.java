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


import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.mongodb.entity.MongoCell;
import com.stratio.deep.mongodb.extractor.MongoCellExtractor;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.utils.ContextProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.stratio.deep.commons.extractor.server.ExtractorServer.initExtractorServer;
import static com.stratio.deep.commons.extractor.server.ExtractorServer.stopExtractorServer;

/**
 * Created by rcrespo on 25/06/14.
 */
public final class GroupingCellWithMongoDB {


    private GroupingCellWithMongoDB() {
    }


    public static void main(String[] args) {
        doMain(args);
    }


    public static void doMain(String[] args) {
        String job = "scala:groupingCellWithMongoDB";

        String host = "localhost:27017";

        String database = "book";
        String inputCollection = "input";
        String outputCollection = "output";


        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
	    DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());


        initExtractorServer();

        ExtractorConfig<Cells> inputConfigEntity = new ExtractorConfig<>();
        inputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database).putValue(ExtractorConstants.COLLECTION, inputCollection);
        inputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);

        RDD<Cells> inputRDDEntity = deepContext.createRDD(inputConfigEntity);


        JavaRDD<String> words =inputRDDEntity.toJavaRDD().flatMap(new FlatMapFunction<Cells, String>() {
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
                return new Cells(MongoCell.create("word", stringIntegerTuple2._1()) , MongoCell.create("count", stringIntegerTuple2._2()));
            }
        });


        ExtractorConfig<Cells> outputConfigEntity = new ExtractorConfig<>();
        outputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database).putValue(ExtractorConstants.COLLECTION, outputCollection);
        outputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);


        deepContext.saveRDD(outputRDD.rdd(), outputConfigEntity);

        stopExtractorServer();
        deepContext.stop();

    }




}
