/*
Copyright 2014 Stratio.

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.
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

    private CreatingCellRDD(){}

    public static void main(String[] args) {

        String job = "java:creatingCellRDD";

        String keyspaceName = "tutorials";
        String tableName = "tweets";

        // Creating the Deep Context
        ContextProperties p = new ContextProperties();
        DeepSparkContext deepContext = new DeepSparkContext(p.cluster, job, p.sparkHome, p.jarList);

// Configuration and initialization
        IDeepJobConfig config = DeepJobConfigFactory.create()
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(keyspaceName).table(tableName)
                .initialize();

// Creating the RDD
        CassandraJavaRDD rdd = deepContext.cassandraJavaRDD(config);

        long counts = rdd.count();

        logger.info("Num of rows: " + counts);

        System.exit(0);

    }
}
