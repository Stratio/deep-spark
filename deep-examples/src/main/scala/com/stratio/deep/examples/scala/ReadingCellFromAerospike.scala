package com.stratio.deep.examples.scala

import com.stratio.deep.aerospike.config.{AerospikeConfigFactory, AerospikeDeepJobConfig}
import com.stratio.deep.commons.entity.Cells
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties
import org.apache.spark.rdd.RDD

/**
 * Example class to read a cell from Aerospike
 */
object ReadingCellFromAerospike {

  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "scala:readingCellFromAerospike"
    val host: String = "localhost"
    val port: Integer = 3000
    val namespace: String = "test"
    val set: String = "input"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: AerospikeDeepJobConfig[Cells] = AerospikeConfigFactory.createAerospike().host(host).port(port).namespace(namespace).set(set).initialize

    val inputRDDEntity: RDD[Cells] = deepContext.createJavaRDD(inputConfigEntity)

    System.out.println("count : " + inputRDDEntity.cache.count)

    deepContext.stop

  }


}
