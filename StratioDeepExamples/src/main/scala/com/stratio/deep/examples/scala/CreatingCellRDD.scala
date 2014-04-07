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

package com.stratio.deep.examples.scala

import com.stratio.deep.context._
import com.stratio.deep.config._
import com.stratio.deep.rdd._
import com.stratio.deep.testutils.ContextProperties
import com.stratio.deep.entity.Cells

/**
 * Author: Emmanuelle Raffenne
 * Date..: 19-feb-2014
 */

object CreatingCellRDD {

    def main(args:Array[String]) {

        val job = "scala:creatingCellRDD"
        val keyspaceName = "tutorials"
        val tableName = "tweets"

        // Creating the Deep Context where args are Spark Master and Job Name
        val p = new ContextProperties(args)
        val deepContext: DeepSparkContext = new DeepSparkContext(p.getCluster, job)

        // Configuration and initialization
        val config :IDeepJobConfig[Cells] = DeepJobConfigFactory.create()
                .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
                .keyspace(keyspaceName).table(tableName)
                .initialize

        // Creating the RDD
        val rdd: CassandraRDD[Cells] = deepContext.cassandraGenericRDD(config)

        val counts = rdd.count()

        println("Num of rows: " + counts.toString)

        System.exit(0)

    }
}
