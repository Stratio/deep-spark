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

package com.stratio.deep.rdd

import com.stratio.deep.testentity.DeepScalaPageEntity
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
  private var rdd: CassandraRDD[DeepScalaPageEntity] = _
  private var rddConfig: IDeepJobConfig[DeepScalaPageEntity] = _
  private var writeConfig: IDeepJobConfig[DeepScalaPageEntity] = _

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
    val entities: Array[DeepScalaPageEntity] = obj.asInstanceOf[Array[DeepScalaPageEntity]]
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


  private def initWriteConfig(): IDeepJobConfig[DeepScalaPageEntity] = {

    writeConfig =
      DeepJobConfigFactory
        .create(classOf[DeepScalaPageEntity])
        .host(Constants.DEFAULT_CASSANDRA_HOST)
        .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
        .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
        .keyspace(AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME)
        .columnFamily(AbstractDeepSparkContextTest.OUTPUT_COLUMN_FAMILY)
        .batchSize(2)
        .createTableOnWrite(true)

    writeConfig.initialize
  }

  private def initReadConfig(): IDeepJobConfig[DeepScalaPageEntity] = {
    rddConfig =
      DeepJobConfigFactory
        .create(classOf[DeepScalaPageEntity])
        .host(Constants.DEFAULT_CASSANDRA_HOST)
        .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
        .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
        .keyspace(AbstractDeepSparkContextTest.KEYSPACE_NAME)
        .columnFamily(AbstractDeepSparkContextTest.COLUMN_FAMILY)

    rddConfig.initialize()
  }

  private def initRDD(): CassandraRDD[DeepScalaPageEntity] = {
    super.getContext.cassandraEntityRDD(rddConfig);
  }

  private def checkSimpleTestData(): Unit = {

  }

  private def checkComputedData(entities: Array[DeepScalaPageEntity]): Unit = {

  }
}
