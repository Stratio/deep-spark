package com.stratio.deep.rdd

import com.stratio.deep.entity.{ScalaPageEntity, TestEntity}
import com.stratio.deep.config.{DeepJobConfigFactory, IDeepJobConfig}
import com.stratio.deep.util.Constants
import com.stratio.deep.embedded.CassandraServer
import com.stratio.deep.context.AbstractDeepSparkContextTest
import org.testng.annotations.{BeforeClass, Test}
import org.testng.Assert._
import org.apache.spark.Partition

/**
 * Created by luca on 20/03/14.
 */
@Test (suiteName = "cassandraRddTests", dependsOnGroups = Array ("CassandraJavaRDDTest"), groups = Array ("ScalaCassandraEntityRDDTest") )
class ScalaCassandraEntityRDDTest extends AbstractDeepSparkContextTest {
  private var rdd: CassandraRDD[ScalaPageEntity] = _
  private var rddConfig: IDeepJobConfig[ScalaPageEntity] = _
  private var writeConfig: IDeepJobConfig[ScalaPageEntity] = _

  @BeforeClass
  protected def initServerAndRDD {
    rddConfig = initReadConfig
    writeConfig = initWriteConfig
    rdd = initRDD
  }

  @Test(dependsOnMethods = Array("testGetPreferredLocations"))
  def testCompute {
    val obj: AnyRef = rdd.collect
    assertNotNull(obj)
    val entities: Array[ScalaPageEntity] = obj.asInstanceOf[Array[ScalaPageEntity]]
    checkComputedData(entities)
  }

  @Test(dependsOnMethods = Array("testRDDInstantiation")) def testGetPartitions {
    val partitions: Array[Partition] = rdd.partitions
    assertNotNull(partitions)
    assertEquals(partitions.length, 8 + 1)
  }

  @Test(dependsOnMethods = Array("testGetPartitions")) def testGetPreferredLocations {
    val partitions: Array[Partition] = rdd.partitions
    val locations: Seq[String] = rdd.getPreferredLocations(partitions(0))
    assertNotNull(locations)
  }

  @Test def testRDDInstantiation {
    assertNotNull(rdd)
  }

  @Test
  def testCql3SaveToCassandra(): Unit = {

  }

  @Test
  def testSimpleSaveToCassandra(): Unit = {

  }

  @Test
  def testSaveToCassandra(): Unit = {

  }


  private def initWriteConfig(): IDeepJobConfig[ScalaPageEntity] = {

    writeConfig =
      DeepJobConfigFactory
        .create(classOf[ScalaPageEntity])
        .host(Constants.DEFAULT_CASSANDRA_HOST)
        .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
        .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
        .keyspace(AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME)
        .columnFamily(AbstractDeepSparkContextTest.OUTPUT_COLUMN_FAMILY)
        .batchSize(2)
        .createTableOnWrite(true)

    writeConfig.initialize
  }

  private def initReadConfig(): IDeepJobConfig[ScalaPageEntity] = {
    rddConfig =
      DeepJobConfigFactory
        .create(classOf[ScalaPageEntity])
        .host(Constants.DEFAULT_CASSANDRA_HOST)
        .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
        .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
        .keyspace(AbstractDeepSparkContextTest.KEYSPACE_NAME)
        .columnFamily(AbstractDeepSparkContextTest.COLUMN_FAMILY)

    rddConfig.initialize()
  }

  private def initRDD(): CassandraRDD[ScalaPageEntity] = {
    super.getContext.cassandraEntityRDD(rddConfig);
  }

  private def checkSimpleTestData(): Unit = {

  }

  private def checkComputedData(entities: Array[ScalaPageEntity]): Unit = {

  }
}
