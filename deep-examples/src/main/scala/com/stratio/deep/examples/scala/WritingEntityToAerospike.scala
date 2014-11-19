package com.stratio.deep.examples.scala

import com.stratio.deep.aerospike.config.{AerospikeConfigFactory, AerospikeDeepJobConfig}
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties
import com.stratio.deep.testentity.MessageEntity
import org.apache.spark.rdd.RDD

/**
 * Example for reading a Stratio entity from Aerospike
 */
object WritingEntityToAerospike {

  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "scala:writingEntityToAerospike"
    val host: String = "localhost"
    val port: Integer = 3000
    val namespace: String = "test"
    val inputSet: String = "message"
    val outputSet: String = "messageOutput"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext: DeepSparkContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: AerospikeDeepJobConfig[MessageEntity] = AerospikeConfigFactory.createAerospike(classOf[MessageEntity]).host(host).port(port).namespace(namespace).set(inputSet).initialize

    val inputRDDEntity: RDD[MessageEntity] = deepContext.createJavaRDD(inputConfigEntity)

    val outputConfigEntity: AerospikeDeepJobConfig[MessageEntity] = AerospikeConfigFactory.createAerospike(classOf[MessageEntity]).host(host).port(port).namespace(namespace).set(outputSet).initialize

    DeepSparkContext.saveRDD(inputRDDEntity, outputConfigEntity)

    deepContext.stop

  }

}
