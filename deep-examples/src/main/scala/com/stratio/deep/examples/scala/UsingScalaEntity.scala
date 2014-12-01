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

import com.stratio.deep.cassandra.config.{CassandraConfigFactory, CassandraDeepJobConfig}
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.testentity.{ScalaOutputEntity, ScalaPageEntity}
import com.stratio.deep.utils.ContextProperties
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math._

/**
 * Author: Luca Rosellini
 * Date..: 19-mar-2014
 */

object UsingScalaEntity {
  type T = (String, Int)
  type U = (Int, Int, Double)

  def jobName = "scala:usingScalaEntity";

  def keyspaceName = "crawler";

  def tableName = "Page";

  def outputTableName = "out_page";

  // context properties

  def main(args: Array[String]) {
    val p = new ContextProperties(args);

    val sparkConf = new SparkConf()
    sparkConf.setAppName(jobName)
    sparkConf.setJars(p.getJars)
    sparkConf.setMaster(p.getCluster)
    sparkConf.setSparkHome(p.getSparkHome)
    val sc = new SparkContext(sparkConf)
    val deepContext: DeepSparkContext = new DeepSparkContext(sc);
    doMain(args, deepContext, p)
  }

  private def doMain(args: Array[String], deepContext: DeepSparkContext, p: ContextProperties) = {

    // Configuration and initialization
    val config: CassandraDeepJobConfig[ScalaPageEntity] = CassandraConfigFactory.create(classOf[ScalaPageEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(keyspaceName).table(tableName)
      .initialize

    // Creating the RDD
    val rdd = deepContext.createRDD(config)

    val rddCount: Long = rdd.count

    println("rddCount: " + rddCount)

    // grouping
    val groups: RDD[(String, Iterable[ScalaPageEntity])] = rdd groupBy {
      t: ScalaPageEntity => t.getDomain
    }

    // counting elements in groups
    val counts: RDD[T] = groups map {
      t: (String, Iterable[ScalaPageEntity]) => (t._1, t._2.size)
    }

    // fetching results
    val result: Array[T] = counts.collect()

    result.foreach {
      e => println(e)
    }

    // creating a key-value pairs RDD
    val pairsRDD: RDD[(String, ScalaPageEntity)] = rdd map {
      e: ScalaPageEntity => (e.getDomain, e)
    }

    // grouping by key
    val groups2: RDD[(String, Iterable[ScalaPageEntity])] = pairsRDD.groupByKey

    // counting elements in groups
    val counts2: RDD[T] = groups2 map {
      t: (String, Iterable[ScalaPageEntity]) => (t._1, t._2.size)
    }

    // fetching results
    val result2: Array[T] = counts2.collect()

    result2.foreach {
      e => println(e)
    }

    // Map stage: Getting key-value pairs from the RDD
    val pairsRDD3: RDD[T] = rdd map {
      e: ScalaPageEntity => (e.getDomain, 1)
    }

    // Reduce stage: counting rows
    val counts3: RDD[T] = pairsRDD3 reduceByKey {
      _ + _
    }

    // Fetching the results
    val results3: Array[(String, Int)] = counts3.collect()

    results3.foreach {
      e => println(e)
    }

    // --- OUTPUT RDD
    val outputConfig = CassandraConfigFactory.create(classOf[ScalaOutputEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort)
      .keyspace(keyspaceName).table(outputTableName)
      .createTableOnWrite(true)
      .initialize


    val outputRDD: RDD[ScalaOutputEntity] = counts3 map {
      t: (String, Int) =>
        val out = new ScalaOutputEntity();
        out.setKey(t._1);
        out.setValue(t._2);
        out
    }
    // Write to Cassandra
    DeepSparkContext.saveRDD(outputRDD, outputConfig)

    // aggregating
    val (sumOfX, n, sumOfSquares): U = counts3.aggregate(0: Int, 0: Int, 0: Double)({
      (accumulator: U, elem: T) => (accumulator._1 + elem._2, accumulator._2 + 1, accumulator._3 + pow(elem._2, 2)) // converts T to U

    }, {
      (u: U, v: U) =>
        (u._1 + v._1, u._2 + v._2, u._3 + v._3) // aggregates two Us to a single U
    })

    // computing stats
    val avg: Double = sumOfX.toDouble / n.toDouble
    val variance: Double = (sumOfSquares.toDouble / n.toDouble) - pow(avg, 2)
    val stddev: Double = sqrt(variance)

    println("avg: " + avg)
    println("variance: " + variance)
    println("stddev: " + stddev)
    deepContext.stop
  }

}
