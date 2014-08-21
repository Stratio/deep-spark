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

import com.stratio.deep.config.CassandraConfigFactory;
import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraJavaRDD;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 19-feb-2014
 */
public final class CreatingCellRDD {
    private static final Logger LOG = Logger.getLogger(CreatingCellRDD.class);

    private static Long counts;

    private CreatingCellRDD() {
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
        String job = "java:creatingCellRDD";

        String keyspaceName = "test";
        String tableName = "tweets";

        // Creating the Deep Context
        ContextProperties p = new ContextProperties(args);
        SparkConf sparkConf = new SparkConf()
                .setMaster(p.getCluster())
                .setAppName(job)
                .setJars(p.getJars())
                .setSparkHome(p.getSparkHome())
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator","com.stratio.deep.serializer.DeepKryoRegistrator");

        SparkContext sc = new SparkContext(p.getCluster(), job, sparkConf);

        LOG.info("spark.serializer: " + System.getProperty("spark.serializer"));
        LOG.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

	    DeepSparkContext deepContext = new DeepSparkContext(sc);

        // Configuration and initialization
        DeepJobConfig<Cells> config = CassandraConfigFactory.create()
                .host(p.getCassandraHost())
                .cqlPort(p.getCassandraCqlPort())
                .rpcPort(p.getCassandraThriftPort())
                .keyspace(keyspaceName)
                .table(tableName)
                .initialize();

        // Creating the RDD
        RDD rdd =  deepContext.createRDD(config);

        counts = rdd.count();

        LOG.info("Num of rows: " + counts);

        deepContext.stop();
    }

    public static Long getCounts() {
        return counts;
    }
}
