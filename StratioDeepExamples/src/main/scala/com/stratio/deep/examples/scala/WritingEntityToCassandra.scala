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
package com.stratio.deep.examples.scala

import org.apache.spark.SparkContext._
import com.stratio.deep.config._
import com.stratio.deep.context._
import com.stratio.deep.rdd.CassandraRDD
import com.stratio.deep.entity._
import com.stratio.deep.rdd._
import org.apache.spark.rdd.RDD
import com.stratio.deep.utils.ContextProperties
import com.stratio.deep.entity.{PageEntity, DomainEntity}

/**
 * Author: Emmanuelle Raffenne
 * Date..: 12-feb-2014
 */

object WritingEntityToCassandra {

    def main (args:Array[String]) {

        val job = "scala:writingEntityToCassandra"

        val inputKeyspaceName = "crawler"
        val inputTableName = "Page"
        val outputKeyspaceName = "crawler"
        val outputTableName = "listdomains"

        // Creating the Deep Context where args are Spark Master and Job Name
        val p = new ContextProperties
        val deepContext: DeepSparkContext = new DeepSparkContext(p.cluster, job, p.sparkHome, p.jarList)

        // --- INPUT RDD
        val inputConfig = DeepJobConfigFactory.create(classOf[PageEntity])
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(inputKeyspaceName).table(inputTableName)
                .initialize

        val inputRDD: CassandraRDD[PageEntity] = deepContext.cassandraEntityRDD(inputConfig)

        val pairRDD: RDD[(String, PageEntity)] = inputRDD map {e:PageEntity => (e.getDomainName, e)}

        val numPerKey: RDD[(String, Int)] = pairRDD.groupByKey
                .map { t:(String, Seq[PageEntity]) => (t._1, t._2.size)}


        // -------------------------------- OUTPUT to Cassandra
        // Creating a configuration for the output RDD and initialize it
        // --- OUTPUT RDD
        val outputConfig = DeepJobConfigFactory.create(classOf[DomainEntity])
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(outputKeyspaceName).table(outputTableName).createTableOnWrite(true)
                .initialize

        val outputRDD: RDD[DomainEntity] = numPerKey map { t: (String, Int) =>
            val out = new DomainEntity();
            out.setDomain(t._1);
            out.setNumPages(t._2);
            out
        }

        CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig)

        System.exit(0)
    }

}
