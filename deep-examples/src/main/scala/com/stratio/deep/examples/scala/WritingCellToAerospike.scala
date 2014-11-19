package com.stratio.deep.examples.scala

import com.stratio.deep.aerospike.config.{AerospikeConfigFactory, AerospikeDeepJobConfig}
import com.stratio.deep.commons.entity.Cells
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties
import org.apache.spark.rdd.RDD

/**
 * Example class to read a cell from Aerospike
 */
object WritingCellToAerospike {

  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "scala:writingCellToAerospike"
    val host: String = "localhost"
    val port: Integer = 3000
    val namespace: String = "test"
    val inputSet: String = "input"
    val outputSet: String = "output"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext: DeepSparkContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: AerospikeDeepJobConfig[Cells] = AerospikeConfigFactory.createAerospike().host(host).port(port).namespace(namespace).set(inputSet)

    val inputRDDEntity: RDD[Cells] = deepContext.createJavaRDD(inputConfigEntity)

    val outputConfigEntity: AerospikeDeepJobConfig[Cells] = AerospikeConfigFactory.createAerospike().host(host).port(port).namespace(namespace).set(outputSet)

    DeepSparkContext.saveRDD(inputRDDEntity, outputConfigEntity)

    deepContext.stop
  }

}
