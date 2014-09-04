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


import com.datastax.driver.core.{Cluster, ResultSet, Row, Session}
import com.stratio.deep.commons.config.ExtractorConfig
import com.stratio.deep.commons.extractor.utils.ExtractorConstants
import com.stratio.deep.commons.utils.{Constants, Utils}
import com.stratio.deep.core.context.AbstractDeepSparkContextTest
import com.stratio.deep.embedded.CassandraServer
import com.stratio.deep.testentity.DeepScalaPageEntity
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.testng.Assert._
import org.testng.annotations.{BeforeClass, Test}


/**
 * Created by luca on 20/03/14.
 */
@Test(suiteName = "cassandraRddTests", dependsOnGroups = Array("CassandraJavaRDDTest"), groups = Array("ScalaCassandraEntityRDDTest"))
class ScalaCassandraEntityRDDTest extends AbstractDeepSparkContextTest {
  private var rdd: RDD[DeepScalaPageEntity] = _
  private var rddConfig: ExtractorConfig[DeepScalaPageEntity] = _
  private var writeConfig: ExtractorConfig[DeepScalaPageEntity] = _
  private val OUTPUT_COLUMN_FAMILY: String = "out_scalatest_page"

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

//  @Test(dependsOnMethods = Array("testGetPartitions")) def testGetPreferredLocations {
//    val partitions: Array[Partition] = rdd.partitions
//    val locations: Seq[String] = rdd.getPreferredLocations(partitions(0))
//    assertNotNull(locations)
//  }

  @Test def testRDDInstantiation {
    assertNotNull(rdd)
  }

  @Test
  def testCql3SaveToCassandra(): Unit = {

  }

  @Test
  def testSimpleSaveToCassandra(): Unit = {

    try {
      AbstractDeepSparkContextTest.executeCustomCQL("DROP TABLE " +
        Utils.quote(AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME) + "." + Utils.quote(OUTPUT_COLUMN_FAMILY))
    }
    catch {
      case e: Exception =>

    };

    //CassandraRDD.saveRDDToCassandra(rdd, writeConfig)
    checkSimpleTestData()
  }

  private def initWriteConfig(): ExtractorConfig[DeepScalaPageEntity] = {
    writeConfig =new ExtractorConfig[DeepScalaPageEntity]()

    val values: java.util.Map[String, String] = new java.util.HashMap[String, String]
    writeConfig.putValue(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
    writeConfig.putValue(ExtractorConstants.KEYSPACE, AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME)
    writeConfig.putValue(ExtractorConstants.COLUMN_FAMILY, OUTPUT_COLUMN_FAMILY)
    writeConfig.putValue(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT))
    writeConfig.putValue(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT))
    writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, String.valueOf(true))
    writeConfig.putValue(ExtractorConstants.BATCHSIZE, String.valueOf(2))

    //writeConfig.setValues(values)

  }

  private def initReadConfig(): ExtractorConfig[DeepScalaPageEntity] = {
    rddConfig =new ExtractorConfig[DeepScalaPageEntity]()

    val values: java.util.Map[String, String] = new java.util.HashMap[String, String]
    rddConfig.putValue(ExtractorConstants.KEYSPACE, AbstractDeepSparkContextTest.KEYSPACE_NAME)
    rddConfig.putValue(ExtractorConstants.COLUMN_FAMILY, AbstractDeepSparkContextTest.COLUMN_FAMILY)
    rddConfig.putValue(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT))
    rddConfig.putValue(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT))
    rddConfig.putValue(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST)

//    rddConfig.setValues(values)


  } 

  private def initRDD(): RDD[DeepScalaPageEntity] = {
    getContext
      .createRDD(rddConfig).asInstanceOf[RDD[DeepScalaPageEntity]]
  }

  private def checkSimpleTestData(): Unit = {
    val cluster: Cluster = Cluster.builder.withPort(CassandraServer.CASSANDRA_CQL_PORT).addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build
    val session: Session = cluster.connect
    var command: String = "select count(*) from " +
      Utils.quote(AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME) + "." +
      Utils.quote(AbstractDeepSparkContextTest.OUTPUT_COLUMN_FAMILY) + ";"
    var rs: ResultSet = session.execute(command)
    assertEquals(rs.one.getLong(0), AbstractDeepSparkContextTest.entityTestDataSize)
    command = "select * from " +
      Utils.quote(AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME) + "." +
        Utils.quote(AbstractDeepSparkContextTest.OUTPUT_COLUMN_FAMILY) + " WHERE \"id\" = 'e71aa3103bb4a63b9e7d3aa081c1dc5ddef85fa7';"
    rs = session.execute(command)
    val row: Row = rs.one
    assertEquals(row.getString("domain_name"), "11870.com")
    assertEquals(row.getInt("response_time"), 421)
    assertEquals(row.getLong("download_time"), 1380802049275L)
    assertEquals(row.getString("url"), "http://11870.com/k/es/de")
    session.close
  }

  private def checkComputedData(entities: Array[DeepScalaPageEntity]): Unit = {

  }
}
