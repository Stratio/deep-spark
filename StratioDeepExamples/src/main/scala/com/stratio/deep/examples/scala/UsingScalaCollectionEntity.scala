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
import com.stratio.deep.config.IDeepJobConfig
import com.stratio.deep.config.DeepJobConfigFactory
import org.apache.spark.{SparkContext, SparkConf}

import com.stratio.deep.testutils.ContextProperties
import com.stratio.deep.testentity.{ScalaOutputEntity, ScalaCollectionEntity}
import com.stratio.deep.rdd.CassandraRDD
import org.apache.spark.rdd.RDD

/**
 * Author: Luca Rosellini
 * Date..: 19-mar-2014
 */

object UsingScalaCollectionEntity {
  type T = (String, Int)
  type U = (Int, Int, Double)

  def jobName = "scala:usingScalaCollectionEntity";

  def keyspaceName = "crawler";

  def tableName = "cql3_collection_cf";

  def outputTableName = "out_cql3_collection_cf";

  // context properties

  def main(args: Array[String]) {
    val p = new ContextProperties(args);
    val sparkConf = new SparkConf()
    sparkConf.setAppName(jobName)
    sparkConf.setJars(Array(p.getJar))
    sparkConf.setMaster(p.getCluster)
    sparkConf.setSparkHome(p.getSparkHome)
    val sc = new SparkContext(sparkConf)
    val deepContext: DeepSparkContext = new DeepSparkContext(sc);

    try {
      doMain(args, deepContext, p)
    } finally {
      deepContext.stop
      System.exit(0)
    }
  }

  private def doMain(args: Array[String], deepContext: DeepSparkContext, p:ContextProperties) = {

    // Configuration and initialization
    val config: IDeepJobConfig[ScalaCollectionEntity] = DeepJobConfigFactory.create(classOf[ScalaCollectionEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(keyspaceName).table(tableName)
      .initialize

    // Creating the RDD
    val rdd = deepContext.cassandraEntityRDD(config)

    val rddCount: Long = rdd.count

    println("rddCount: " + rddCount)

    // group by the first email domain name...
    val grouped = rdd.groupBy {
      e: ScalaCollectionEntity =>
        val firstEmail = e.getEmails.iterator().next()
        firstEmail.substring(firstEmail.indexOf("@") + 1)
    } map {
      t => (t._1, t._2.size)
    } // ... and then count the number of emails for the same domain

    grouped.collect().sortBy {
      e => e._2
    } foreach {
      e => println(e)
    }

    // let's write the result to Cassandra
    val writeConfig: IDeepJobConfig[ScalaOutputEntity] = DeepJobConfigFactory.create(classOf[ScalaOutputEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort)
      .keyspace(keyspaceName).table(outputTableName).createTableOnWrite(true)
      .initialize

    val outputRDD: RDD[ScalaOutputEntity] = grouped map {
      t: (String, Int) =>
        val out = new ScalaOutputEntity();
        out.setKey(t._1);
        out.setValue(t._2);
        out
    }

    CassandraRDD.saveRDDToCassandra(outputRDD, writeConfig)
  }

}
