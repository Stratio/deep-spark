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

package com.stratio.deep.examples.java.savewithfunction;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.utils.ContextProperties;

import scala.Tuple2;

public class WrittingRddWithDefaultQueryBuilderToCassandra {
    private static final Logger LOG = Logger.getLogger(WrittingRddWithDefaultQueryBuilderToCassandra.class);

    private WrittingRddWithDefaultQueryBuilderToCassandra() {
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
        String job = "java:saveWithQueryBuilder";

        String keyspaceName = "test";
        String inputTableName = "tweets2";
        final String outputTableName = "copy_tweets3";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        // --- INPUT RDD
        CassandraDeepJobConfig<Cells> inputConfig = CassandraConfigFactory.create()
                .host(p.getCassandraHost()).cqlPort(p.getCassandraCqlPort()).rpcPort(p.getCassandraThriftPort())
                .keyspace(keyspaceName).table(inputTableName)
                .initialize();

        long initTime = System.currentTimeMillis();

        JavaRDD<Cells> inputRDD = deepContext.createJavaRDD(inputConfig);

        System.out.println("**********************" + inputRDD.count() + System.currentTimeMillis());
        long timeCreate = System.currentTimeMillis() - initTime;
        initTime = System.currentTimeMillis();

        // --- OUTPUT RDD
        CassandraDeepJobConfig<Cells> outputConfig = CassandraConfigFactory.create()
                .host(p.getCassandraHost()).cqlPort(p.getCassandraCqlPort()).rpcPort(p.getCassandraThriftPort())
                .keyspace(keyspaceName).table(outputTableName).createTableOnWrite(true)
                .initialize();

        deepContext.saveRDD(inputRDD.rdd(), outputConfig);

        System.out.println("**********************");
        long timeSave = System.currentTimeMillis() - initTime;
        initTime = System.currentTimeMillis();
        System.out.println("initTime" + timeCreate + "save" + timeSave);

        deepContext.stop();
    }
}
