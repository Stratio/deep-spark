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


import com.datastax.driver.core.ResultSet;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.utils.Constants;
import org.testng.annotations.Test;
import scala.Tuple2;

import static org.testng.Assert.assertEquals;

/**
 * Test for the AggregatingData standalone example.
 */
@Test(suiteName = "deepExamplesTests", groups = {"DeepExamplesTest"})
public class DeepExamplesTest extends AbstractDeepExamplesTest {
    String[] args = new String[]{
            "-m",
            "local",
            "-cassandraHost",
            Constants.DEFAULT_CASSANDRA_HOST,
            "-cassandraCqlPort",
            "" + CassandraServer.CASSANDRA_CQL_PORT,
            "-cassandraThriftPort",
            "" + CassandraServer.CASSANDRA_THRIFT_PORT};

    @Test
    public void testAggregatingData() {
        AggregatingData.doMain(args);

        assertEquals(AggregatingData.getCount(), 4892.0);
        assertEquals(AggregatingData.getAvg(), 14.262390670553936);
        assertEquals(AggregatingData.getVariance(), 14.251850844461075);
        assertEquals(AggregatingData.getStddev(), 3.775162360013285);
    }

    @Test(dependsOnMethods = "testAggregatingData")
    public void testCreatingCellRDD() {
        CreatingCellRDD.doMain(args);
        assertEquals(CreatingCellRDD.getCounts(), Long.valueOf("4892"));
    }

    @Test(dependsOnMethods = "testCreatingCellRDD")
    public void testGroupingByColumn() {
        GroupingByColumn.doMain(args);
        assertEquals(GroupingByColumn.getResults().size(), 343);
        for (Tuple2 t : GroupingByColumn.getResults()) {
            if (t._1().equals("id355")) {
                assertEquals(t._2(), 13);
                break;
            }

        }
    }

    @Test(dependsOnMethods = "testGroupingByColumn")
    public void testGroupingByKey() {
        GroupingByKey.doMain(args);
        assertEquals(GroupingByKey.getResult().size(), 343);
        assertEquals(GroupingByKey.getAuthors(), 343);
        assertEquals(GroupingByKey.getTotal(), 4892);
        for (Tuple2 t : GroupingByKey.getResult()) {
            if (t._1().equals("id355")) {
                assertEquals(t._2(), 13);
                break;
            }

        }

    }

    @Test(dependsOnMethods = "testGroupingByKey")
    public void testMapReduceJob() {
        MapReduceJob.doMain(args);
        assertEquals(MapReduceJob.results.size(), 343);
        for (Tuple2 t : MapReduceJob.results) {
            if (t._1().equals("id498")) {
                assertEquals(t._2(), 12);
                break;
            }

        }

    }

    @Test(dependsOnMethods = "testMapReduceJob")
    public void testWritingCellToCassandra() {
        WritingCellToCassandra.doMain(args);
        assertEquals(WritingCellToCassandra.results.size(), 26);

        for (Tuple2 t : WritingCellToCassandra.results) {
            if (t._1().equals("blogs.elpais.com")) {
                assertEquals(t._2(), 3);
            }
            if (t._1().equals("deportebase.elnortedecastilla.es")) {
                assertEquals(t._2(), 2);
            }
        }

        String command = "select count(*) from " + CRAWLER_KEYSPACE_NAME + ".newlistdomains;";
        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 26);
    }

    @Test(dependsOnMethods = "testWritingCellToCassandra")
    public void testWritingEntityToCassandra() {
        WritingEntityToCassandra.doMain(args);
        assertEquals(WritingEntityToCassandra.results.size(), 26);

        for (Tuple2 t : WritingEntityToCassandra.results) {
            if (t._1().equals("blogs.elpais.com")) {
                assertEquals(t._2(), 3);
            }
            if (t._1().equals("deportebase.elnortedecastilla.es")) {
                assertEquals(t._2(), 2);
            }
        }

        String command = "select count(*) from " + CRAWLER_KEYSPACE_NAME + ".listdomains;";
        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 26);
    }
}
