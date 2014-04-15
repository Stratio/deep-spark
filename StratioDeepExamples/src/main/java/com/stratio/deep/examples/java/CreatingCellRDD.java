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
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.rdd.*;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 19-feb-2014
 */
public final class CreatingCellRDD {
    private static Logger logger = Logger.getLogger(CreatingCellRDD.class);

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

        System.exit(0);
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
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), new String[]{p.getJar()});

        // Configuration and initialization
        IDeepJobConfig config = DeepJobConfigFactory.create()
                .host(p.getCassandraHost()).cqlPort(p.getCassandraCqlPort()).rpcPort(p.getCassandraThriftPort())
                .keyspace(keyspaceName).table(tableName)
                .initialize();

        // Creating the RDD
        CassandraJavaRDD rdd = deepContext.cassandraJavaRDD(config);

        counts = rdd.count();

        logger.info("Num of rows: " + counts);

        deepContext.stop();
    }

    public static Long getCounts() {
        return counts;
    }
}
