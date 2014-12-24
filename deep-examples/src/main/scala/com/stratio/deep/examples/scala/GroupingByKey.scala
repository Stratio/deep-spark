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


import com.stratio.deep.cassandra.config.CassandraConfigFactory
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.testentity.TweetEntity
import com.stratio.deep.utils.ContextProperties
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Author: Emmanuelle Raffenne
 * Date..: 3-mar-2014
 */

object GroupingByKey {

  def main(args: Array[String]) {

    val job = "scala:groupingByKey"

    val keyspaceName = "test"
    val tableName = "tweets"

    // Creating the Deep Context where args are Spark Master and Job Name
    val p = new ContextProperties(args)
    val deepContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    // Creating a configuration for the RDD and initialize it
    val config = CassandraConfigFactory.create(classOf[TweetEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(keyspaceName).table(tableName)
      .initialize

    // Creating the RDD
    val rdd: RDD[TweetEntity] = deepContext.createRDD(config)

    // creating a key-value pairs RDD
    val pairsRDD: RDD[(String, TweetEntity)] = rdd map {
      e: TweetEntity => (e.getAuthor, e)
    }

    // grouping by key
    val groups: RDD[(String, Iterable[TweetEntity])] = pairsRDD.groupByKey

    // counting elements in groups
    val counts: RDD[(String, Int)] = groups map {
      t: (String, Iterable[TweetEntity]) => (t._1, t._2.size)
    }

    // fetching results
    val result: Array[(String, Int)] = counts.collect()
    var total: Int = 0
    var authors: Int = 0
    println("This is the groupByKey")
    result foreach {
      t: (String, Int) =>
        println(t._1 + ": " + t._2.toString)
        total = total + t._2
        authors = authors + 1
    }

    println(" total: " + total.toString + "  authors: " + authors.toString)
  }
}
