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

// for implicit functions

import org.apache.spark.rdd.RDD
import com.stratio.deep.testutils.ContextProperties
import com.stratio.deep.testentity.TweetEntity

// for deep

import com.stratio.deep.config._
import com.stratio.deep.context._
import com.stratio.deep.rdd._

// for POJOs

// for math stuff

import java.lang.Math.pow
import java.lang.Math.sqrt

/**
 * Author: Emmanuelle Raffenne
 * Date..: 13-feb-2014
 */
object AggregatingData {

  def main(args: Array[String]) {

    val job = "scala:aggregatingData"
    val keyspaceName = "test"
    val tableName = "tweets"

    // Creating the Deep Context where args are Spark Master and Job Name
    val p = new ContextProperties(args)
    val deepContext: DeepSparkContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome,p.getJars)

    // Creating a configuration for the RDD and initialize it
    val config = DeepJobConfigFactory.create(classOf[TweetEntity])
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(keyspaceName).table(tableName)
      .initialize

    // Creating the RDD
    val rdd: CassandraRDD[TweetEntity] = deepContext.cassandraEntityRDD(config)

    // grouping to get key-value pairs
    val groups: RDD[(String, Int)] = rdd.groupBy {
      t: TweetEntity => t.getAuthor
    }
      .map {
      t: (String, Iterable[TweetEntity]) => (t._1, t._2.size)
    }

    // aggregating
    val (sumOfX, n, sumOfSquares): (Int, Int, Double) = groups.aggregate(0: Int, 0: Int, 0: Double)({
      (n: (Int, Int, Double), t: (String, Int)) =>
        (n._1 + t._2, n._2 + 1, n._3 + pow(t._2, 2))
    }, {
      (u: (Int, Int, Double), v: (Int, Int, Double)) =>
        (u._1 + v._1, u._2 + v._2, u._3 + v._3)
    })

    // computing stats
    val avg: Double = sumOfX.toDouble / n.toDouble
    val variance: Double = (sumOfSquares.toDouble / n.toDouble) - pow(avg, 2)
    val stddev: Double = sqrt(variance)

    println("Results: (" + sumOfX + ", " + n + ", " + sumOfSquares + ")")
    println("average: " + avg.toString)
    println("variance: " + variance.toString)
    println("stddev: " + stddev.toString)

    // aggregate[U](zeroValue: U)(seqOp: Function2[U, T, U], combOp: Function2[U, U, U]): U
    //
    // Aggregate the elements of each partition, and then the results for all the partitions,
    // using given combine functions and a neutral "zero value". This function can return a
    // different result type, U, than the type of this RDD, T. Thus, we need one operation for
    // merging a T into an U and one operation for merging two U's, as in deep.examples.examples.deep.examples.scala.TraversableOnce.
    // Both of these functions are allowed to modify and return their first argument instead of
    // creating a new U to avoid memory allocation.
  }

}
