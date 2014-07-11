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

import java.util.List

import com.stratio.deep.config.{IMongoDeepJobConfig, ConfigFactory, GenericDeepJobConfigMongoDB}
import com.stratio.deep.context.{MongoDeepSparkContext, DeepSparkContext}
import com.stratio.deep.rdd.mongodb.{MongoEntityRDD, MongoJavaRDD}
import com.stratio.deep.testentity.MessageEntity
import com.stratio.deep.testutils.ContextProperties
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
 * Example class to write an entity to mongoDB
 */
object WritingEntityToMongoDB {
  /**
   * Application entry point.
   *
   * @param args the arguments passed to the application.
   */
  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "scala:writingEntityToMongoDB"
    val host: String = "localhost:27017"
    val database: String = "test"
    val inputCollection: String = "input"
    val outputCollection: String = "output"
    val readPreference: String = "nearest"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext: MongoDeepSparkContext = new MongoDeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: IMongoDeepJobConfig[MessageEntity] = ConfigFactory.createMongoDB(classOf[MessageEntity]).host(host).database(database).collection(inputCollection).readPreference(readPreference).initialize

    val inputRDDEntity: RDD[MessageEntity] = deepContext.mongoJavaRDD(inputConfigEntity)

    val outputConfigEntity: IMongoDeepJobConfig[MessageEntity] = ConfigFactory.createMongoDB(classOf[MessageEntity]).host(host).database(database).collection(outputCollection).readPreference(readPreference).initialize

    MongoEntityRDD.saveEntity(inputRDDEntity, outputConfigEntity)

    deepContext.stop
  }





}
