package com.stratio.deep.examples.scala

import java.util.UUID

import com.stratio.deep.cassandra.config.CassandraConfigFactory
import com.stratio.deep.cassandra.querybuilder.IncreaseCountersQueryBuilder
import com.stratio.deep.commons.entity.{Cell, Cells}
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.testentity.TweetEntity
import com.stratio.deep.utils.ContextProperties
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import DeepSparkContext._
import scala.collection.JavaConverters._
/**
 * Created by david on 13/11/14.
 */
object AggregationTest extends App{

  override def main(args: Array[String]): Unit = {
    val job = "scala:aggregationJobTest"

    val keyspaceName = "test"
    val tableName = "aggregationtest"
    val p = new ContextProperties(args)

    val ctx = new SparkContext(p.getCluster,job,p.getSparkHome,p.getJars)

    //TODO permitir clonar la configuración
    //TODO configuración para escribir y leer es diferente?? (createWriteConfig())
    val inputConfig = CassandraConfigFactory.createWriteConfig
      .host(p.getCassandraHost).cqlPort(p.getCassandraCqlPort).rpcPort(p.getCassandraThriftPort)
      .keyspace(keyspaceName).table(tableName).initialize
    //TODO crear una tabla a partir de los primeros datos recibidos

    //TODO estructura de datos un poco más sencilla
    val cells1 = new Cells()

    cells1.add(Cell.create("id",UUID.randomUUID(),true))
    cells1.add(Cell.create("col1",1))
    cells1.add(Cell.create("col2",2))
    cells1.add(Cell.create("col3",3))

    val cells2 = new Cells()

    cells2.add(Cell.create("id",UUID.randomUUID(),true))
    cells2.add(Cell.create("col1",4))
    cells2.add(Cell.create("col2",5))
    cells2.add(Cell.create("col3",6))

    val cellsSeq = Seq[Cells](cells1,cells2)
    val cellRDD: RDD[Cells] = ctx.parallelize(cellsSeq,1)

    val cellTest = cellRDD.collect();
    //TODO tiene sentido el deepSparkContext?
    saveRDD(cellRDD,inputConfig)

    ctx.stop()

  }
}
