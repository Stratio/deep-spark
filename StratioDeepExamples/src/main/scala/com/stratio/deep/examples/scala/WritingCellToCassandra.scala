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

import com.stratio.deep.context.DeepSparkContext
import com.stratio.deep.config.DeepJobConfigFactory
import com.stratio.deep.testentity._
import com.stratio.deep.rdd.CassandraRDD
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._
import com.stratio.deep.testutils.ContextProperties

/**
 * Author: Emmanuelle Raffenne
 * Date..: 3-mar-2014
 */

object WritingCellToCassandra {
    def main (args:Array[String]) {

        val job = "scala:writingCellsToCassandra"

        val inputKeyspaceName = "crawler"
        val inputTableName = "Page"
        val outputKeyspaceName = "crawler"
        val outputTableName = "newlistdomains"

        // Creating the Deep Context where args are Spark Master and Job Name
        val p = new ContextProperties
        val deepContext: DeepSparkContext = new DeepSparkContext(p.cluster, job, p.sparkHome, p.jarList)

        // --- INPUT RDD
        val inputConfig = DeepJobConfigFactory.create()
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(inputKeyspaceName).table(inputTableName)
                .initialize

        val inputRDD: CassandraRDD[Cells] = deepContext.cassandraGenericRDD(inputConfig)

        val pairRDD: RDD[(String, Cells)] = inputRDD map {
            c:Cells => (c.getCellByName("domainName").getCellValue.asInstanceOf[String], c)
        }

        val numPerKey: RDD[(String, Integer)] = pairRDD.groupByKey
                .map { t:(String, Seq[Cells]) => (t._1, t._2.size)}


        // -------------------------------- OUTPUT to Cassandra
        // Creating a configuration for the output RDD and initialize it
        // --- OUTPUT RDD
        val outputConfig = DeepJobConfigFactory.create()
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(outputKeyspaceName).table(outputTableName).createTableOnWrite(true)
                .initialize

        val outputRDD: RDD[Cells] = numPerKey map { t: (String, Integer) =>
            val c1 = Cell.create("domain", t._1, true, false);
            val c2 = Cell.create("num_pages", t._2);
            new Cells(c1, c2)
        }

        CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig)

        System.exit(0)
    }

}
