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

import org.apache.spark.SparkContext._
import com.stratio.deep.config._
import com.stratio.deep.context._
import com.stratio.deep.rdd._
import org.apache.spark.rdd.RDD
import com.stratio.deep.testutils.ContextProperties
import com.stratio.deep.testentity.{PageEntity, DomainEntity}

/**
 * Author: Emmanuelle Raffenne
 * Date..: 12-feb-2014
 */

object WritingEntityToCassandra {

  def main(args: Array[String]) {

    val job = "scala:writingEntityToCassandra"

    val inputKeyspaceName = "crawler"
    val inputTableName = "Page"
    val outputKeyspaceName = "crawler"
    val outputTableName = "listdomains"

    // Creating the Deep Context where args are Spark Master and Job Name
    val p = new ContextProperties(args)
    val deepContext: CassandraDeepSparkContext = new CassandraDeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    // --- INPUT RDD
    val inputConfig = CassandraConfigFactory.create(classOf[PageEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(inputKeyspaceName).table(inputTableName)
      .initialize

    val inputRDD: RDD[PageEntity] = deepContext.cassandraRDD(inputConfig)

    val pairRDD: RDD[(String, PageEntity)] = inputRDD map {
      e: PageEntity => (e.getDomainName, e)
    }

    val numPerKey: RDD[(String, Int)] = pairRDD.groupByKey
      .map {
      t: (String, Iterable[PageEntity]) => (t._1, t._2.size)
    }


    // -------------------------------- OUTPUT to Cassandra
    // Creating a configuration for the output RDD and initialize it
    // --- OUTPUT RDD
    val outputConfig = CassandraConfigFactory.createWriteConfig(classOf[DomainEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(outputKeyspaceName).table(outputTableName).createTableOnWrite(true)
      .initialize

    val outputRDD: RDD[DomainEntity] = numPerKey map {
      t: (String, Int) => new DomainEntity(t._1, t._2);
    }

    CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig)

    deepContext.stop
  }

}
