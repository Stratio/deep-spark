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
import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.cassandra.functions.CassandraSaveFunction;
import com.stratio.deep.cassandra.functions.IncreaseCounterFunction;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.functions.SaveFunction;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 3-mar-2014
 */
public final class WritingRddWithFunctionToCassandra {
    private static final Logger LOG = Logger.getLogger(WritingRddWithFunctionToCassandra.class);
    public static List<Tuple2<String, Integer>> results;

    private WritingRddWithFunctionToCassandra() {
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
        String job = "java:saveWithFunction";

        String keyspaceName = "test";
        String inputTableName = "tweets";
        final String outputTableName = "counters";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
	    DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        // --- INPUT RDD
        CassandraDeepJobConfig<Cells> inputConfig = CassandraConfigFactory.create()
                .host(p.getCassandraHost()).cqlPort(p.getCassandraCqlPort()).rpcPort(p.getCassandraThriftPort())
                .keyspace(keyspaceName).table(inputTableName)
                .initialize();

        JavaRDD<Cells> inputRDD = deepContext.createJavaRDD(inputConfig);



        // --- OUTPUT RDD
        CassandraDeepJobConfig<Cells> outputConfig = CassandraConfigFactory.createWriteConfig()
                .host(p.getCassandraHost()).cqlPort(p.getCassandraCqlPort()).rpcPort(p.getCassandraThriftPort())
                .keyspace(keyspaceName).table(outputTableName)
                .createTableOnWrite(true);

        outputConfig.initialize();

        Set<String> primaryKeys = new HashSet<>(1);
        primaryKeys.add("tweet_id");

        Set<String> targetColumns = new HashSet<>(1);
        targetColumns.add("favorite_count");

        deepContext.saveRDD(inputRDD.rdd(), new IncreaseCounterFunction(primaryKeys,targetColumns), outputConfig);

        deepContext.stop();
    }
}
