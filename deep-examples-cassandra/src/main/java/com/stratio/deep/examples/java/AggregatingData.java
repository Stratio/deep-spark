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

import com.google.common.collect.Lists;
import com.stratio.deep.cassandra.extractor.CassandraEntityExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.testentity.TweetEntity;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;


import java.util.HashMap;
import java.util.Map;

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

        String keySpace = "test";
        String tableName    = "tweets";
        String cqlPort      = "9042";
        String rcpPort      = "9160";
        String host         = "127.0.0.1";


       //Call async the Extractor netty Server
        ExtractorServer.initExtractorServer();

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<TweetEntity> config = new ExtractorConfig<>(TweetEntity.class);

        config.setExtractorImplClass(CassandraEntityExtractor.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE, keySpace);
        values.put(ExtractorConstants.TABLE,    tableName);
        values.put(ExtractorConstants.CQLPORT,  cqlPort);
        values.put(ExtractorConstants.RPCPORT,  rcpPort);
        values.put(ExtractorConstants.HOST,     host );

        config.setValues(values);


        // Creating the RDD
        JavaRDD<TweetEntity> rdd = deepContext.createJavaRDD(config);

        // grouping to get key-value pairs
        JavaPairRDD<String, Integer> groups = rdd.groupBy(new Function<TweetEntity, String>() {
            @Override
            public String call(TweetEntity tableEntity) {
                return tableEntity.getAuthor();
            }
        }).mapToPair(new PairFunction<Tuple2<String, Iterable<TweetEntity>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<TweetEntity>> t) throws Exception {
                return new Tuple2<>(t._1(), Lists.newArrayList(t._2()).size());
            }
        });

        // aggregating
        Double zero = 0.0;
        Tuple3<Double, Double, Double> initValues = new Tuple3<Double, Double, Double>(zero, zero, zero);
        Tuple3<Double, Double, Double> results = groups.aggregate(initValues,
                new Function2<Tuple3<Double, Double, Double>, Tuple2<String, Integer>, Tuple3<Double, Double,
                        Double>>() {
                    @Override
                    public Tuple3<Double, Double, Double> call(Tuple3<Double, Double, Double> n, Tuple2<String,
                            Integer> t) {
                        Double sumOfX = n._1() + t._2();
                        Double numOfX = n._2() + 1;
                        Double sumOfSquares = n._3() + Math.pow(t._2(), 2);
                        return new Tuple3<>(sumOfX, numOfX, sumOfSquares);
                    }
                }, new Function2<Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>, Tuple3<Double,
                        Double, Double>>() {
                    @Override
                    public Tuple3<Double, Double, Double> call(Tuple3<Double, Double, Double> a, Tuple3<Double,
                            Double, Double> b) {
                        Double sumOfX = a._1() + b._1();
                        Double numOfX = a._2() + b._2();
                        Double sumOfSquares = a._3() + b._3();
                        return new Tuple3<>(sumOfX, numOfX, sumOfSquares);
                    }
                }
        );

        // computing stats
        Double sumOfX = results._1();
        Double numOfX = results._2();
        Double sumOfSquares = results._3();

        count = sumOfX;
        avg = sumOfX / numOfX;
        variance = (sumOfSquares / numOfX) - Math.pow(avg, 2);
        stddev = Math.sqrt(variance);

        LOG.info("Results: (" + sumOfX.toString() + ", " + numOfX.toString() + ", " + sumOfSquares.toString() + ")");
        LOG.info("average: " + avg.toString());
        LOG.info("variance: " + variance.toString());
        LOG.info("stddev: " + stddev.toString());

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
