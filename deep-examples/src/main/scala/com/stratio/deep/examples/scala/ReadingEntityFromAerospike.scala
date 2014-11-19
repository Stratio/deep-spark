package com.stratio.deep.examples.scala

import com.stratio.deep.aerospike.config.{AerospikeConfigFactory, AerospikeDeepJobConfig}
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties
import com.stratio.deep.testentity.MessageEntity
import org.apache.spark.rdd.RDD

/**
 * Example for reading a Stratio Deep entity from Aerospike.
 */
object ReadingEntityFromAerospike {

  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "scala:readingEntityFromAerospike"
    val host: String = "localhost"
    val port: Integer = 3000
    val namespace: String = "test"
    val set: String = "message"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: AerospikeDeepJobConfig[MessageEntity] = AerospikeConfigFactory.createAerospike(classOf[MessageEntity]).host(host).port(port).namespace(namespace).set(set).initialize

    val inputRDDEntity: RDD[MessageEntity] = deepContext.createJavaRDD(inputConfigEntity)

    System.out.println("count : " + inputRDDEntity.cache.count)

    deepContext.stop

  }

}
