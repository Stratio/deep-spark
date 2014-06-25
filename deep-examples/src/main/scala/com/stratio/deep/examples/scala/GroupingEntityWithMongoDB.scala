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

import java.util.{ArrayList, Arrays, List}

import com.stratio.deep.config.{DeepJobConfigFactory, IMongoDeepJobConfig}
import com.stratio.deep.context.DeepSparkContext
import com.stratio.deep.rdd.mongodb.{MongoEntityRDD, MongoJavaRDD}
import com.stratio.deep.testentity.{BookEntity, WordCount}
import com.stratio.deep.testutils.ContextProperties
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.api.java.function.{FlatMapFunction, Function, Function2, PairFunction}

import scala.collection.JavaConversions._

/**
 * Created by rcrespo on 25/06/14.
 */
final object GroupingEntityWithMongoDB {
  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "java:readingEntityFromMongoDB"
    val host: String = "localhost:27017"
    val database: String = "book"
    val inputCollection: String = "input"
    val outputCollection: String = "output"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext: DeepSparkContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: IMongoDeepJobConfig[BookEntity] = DeepJobConfigFactory.createMongoDB(classOf[BookEntity]).host(host).database(database).collection(inputCollection).initialize

    val inputRDDEntity: MongoJavaRDD[BookEntity] = deepContext.mongoJavaRDD(inputConfigEntity)


    val words = inputRDDEntity.flatMap(new FlatMapFunction[BookEntity, String]() {

      override def call(bookEntity: BookEntity): java.lang.Iterable[String] = {
        var words : List[String] = new ArrayList[String]()
        for (canto <- bookEntity.getCantoEntities) {
          words.addAll(Arrays.asList(canto.getText.split(" "):_*))
        }
        return words
      }
    })

    val wordCount: JavaPairRDD[String, Integer] = words.mapToPair(new PairFunction[String, String, Integer] {
      def call(s: String): (String, Integer) = {
        return new (String, Integer)(s, 1)
      }
    })

    val wordCountReduced: JavaPairRDD[String, Integer] = wordCount.reduceByKey(new Function2[Integer, Integer, Integer] {
      def call(integer: Integer, integer2: Integer): Integer = {
        return integer + integer2
      }
    })

    val outputRDD: JavaRDD[WordCount] = wordCountReduced.map(new Function[(String, Integer), WordCount] {
      def call(stringIntegerTuple2: (String, Integer)): WordCount = {
        return new WordCount(stringIntegerTuple2._1, stringIntegerTuple2._2)
      }
    })

    val outputConfigEntity: IMongoDeepJobConfig[WordCount] = DeepJobConfigFactory.createMongoDB(classOf[WordCount]).host(host).database(database).collection(outputCollection).initialize

    MongoEntityRDD.saveEntity(outputRDD, outputConfigEntity)

    deepContext.stop
  }

}
