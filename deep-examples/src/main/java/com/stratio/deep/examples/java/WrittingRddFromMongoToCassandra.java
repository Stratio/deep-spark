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

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.mongodb.config.MongoConfigFactory;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.utils.ContextProperties;

import scala.Tuple2;

public class WrittingRddFromMongoToCassandra {
    private static final Logger LOG = Logger.getLogger(WrittingRddFromMongoToCassandra.class);
    public static List<Tuple2<String, Integer>> results;

    private WrittingRddFromMongoToCassandra() {
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
        String job = "java:saveFromMongoToCassandra";

        String keyspaceName = "test";
        final String outputTableName = "copy_mongo";
        String host = "127.0.0.1:27017";
        String database = "test";
        String inputCollection = "input";

        // Creating the Deep Context where args are Spark Master and Job Name
        com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties p = new com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties(
                args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        // --- INPUT RDD
        MongoDeepJobConfig inputConfigCell = MongoConfigFactory.createMongoDB().host(host).database(database)
                .collection(inputCollection)
                .createInputSplit(false).initialize();

        JavaRDD<Cells> inputRDD = deepContext.createJavaRDD(inputConfigCell);

        for (Cells cells : inputRDD.collect()) {
            cells.getCells();
        }

        // --- OUTPUT RDD

        ContextProperties p2 = new ContextProperties(args);

        CassandraDeepJobConfig<Cells> outputConfig = CassandraConfigFactory.create()
                .host(p2.getCassandraHost()).cqlPort(p2.getCassandraCqlPort()).rpcPort(p2.getCassandraThriftPort())
                .keyspace(keyspaceName).table(outputTableName).createTableOnWrite(true)
                .initialize();

        deepContext.saveRDD(inputRDD.rdd(), outputConfig);

        deepContext.stop();
    }
}
