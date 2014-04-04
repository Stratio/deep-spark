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

package com.stratio.deep.examples.java;


import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.utils.Constants;
import org.testng.annotations.Test;
import scala.Tuple2;

import static org.testng.Assert.*;

/**
 * Test for the AggregatingData standalone example.
 */
@Test(suiteName = "deepExamplesTests", groups = {"DeepExamplesTest"})
public class DeepExamplesTest extends AbstractDeepExamplesTest {
    String[] args = new String[]{
            "local",
            "",
            "",
            Constants.DEFAULT_CASSANDRA_HOST,
            ""+ CassandraServer.CASSANDRA_CQL_PORT,
            ""+ CassandraServer.CASSANDRA_THRIFT_PORT};
    @Test
    public void testAggregatingData(){
        AggregatingData.doMain(args);

        assertEquals(AggregatingData.count, 999.0);
        assertEquals(AggregatingData.avg, 3.1024844720496896);
        assertEquals(AggregatingData.variance, 2.526764013734038);
        assertEquals(AggregatingData.stddev, 1.5895798230142575);
    }

    @Test(dependsOnMethods = "testAggregatingData")
    public void testCreatingCellRDD(){
        CreatingCellRDD.doMain(args);
        assertEquals(AggregatingData.count, 999.0);
    }

    @Test(dependsOnMethods = "testCreatingCellRDD")
    public void testGroupingByColumn(){
        GroupingByColumn.doMain(args);
        assertEquals(GroupingByColumn.results.size(), 322);
        for (Tuple2 t : GroupingByColumn.results) {
            if (t._1().equals("id355")){
                assertEquals(t._2(), 4);
                break;
            }

        }

    }

    @Test(dependsOnMethods = "testGroupingByColumn")
    public void testGroupingByKey(){
        GroupingByKey.doMain(args);
        assertEquals(GroupingByKey.result.size(), 322);
        assertEquals(GroupingByKey.authors, 322);
        assertEquals(GroupingByKey.total, 999);
        for (Tuple2 t : GroupingByKey.result) {
            if (t._1().equals("id355")){
                assertEquals(t._2(), 4);
                break;
            }

        }

    }

    @Test(dependsOnMethods = "testGroupingByKey")
    public void testMapReduceJob(){
        MapReduceJob.doMain(args);
        assertEquals(MapReduceJob.results.size(), 322);
        for (Tuple2 t : GroupingByKey.result) {
            if (t._1().equals("id498")){
                assertEquals(t._2(), 10);
                break;
            }

        }

    }

}
