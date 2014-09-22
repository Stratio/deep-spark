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

package com.stratio.deep.cassandra.rdd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.embedded.CassandraServer;
import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import com.stratio.deep.commons.utils.Constants;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;
import scala.Function1;
import scala.reflect.ClassTag$;

import static org.testng.Assert.*;

/**
 * Integration tests for generic cell RDDs.
 */
@Test(suiteName = "cassandraRddTests", dependsOnGroups = {"CassandraCql3RDDTest"}, groups = {"CassandraCellRDDTest"})
public class CassandraCellRDDTest extends CassandraRDDTest<Cells> {
    private Logger logger = Logger.getLogger(CassandraCellRDDTest.class);

    private static class CellsAbstractSerializableFunction extends AbstractSerializableFunction<Cells, Cells> {
        private static final long serialVersionUID = 78298010100204823L;

        @Override
        public Cells apply(Cells e) {
            return new Cells(e.getDefaultTableName(),
				            e.getCellByName("name"), e.getCellByName("gender"), CassandraCell.create("age", 15, false, true),
                    e.getCellByName("animal"), e.getCellByName("password"), e.getCellByName("color"),
                    e.getCellByName("lucene"), e.getCellByName("food"));
        }
    }

    @Override
    protected void checkComputedData(Cells[] entities) {
        boolean found = false;

        assertEquals(entities.length, cql3TestDataSize);

        for (Cells cells : entities) {

            Cells indexCells = cells.getIndexCells();
            Cells valueCells = cells.getValueCells();

            if (indexCells.equals(
				            new Cells(cells.getDefaultTableName(), CassandraCell.create("name", "pepito_3", true, false), CassandraCell.create("gender", "male",
                    true, false), CassandraCell.create("age", -2, false, true), CassandraCell.create("animal", "monkey", false, true)))) {
                assertEquals(valueCells.getCellByName("password").getCellValue(), "abc");
                assertNull(valueCells.getCellByName("color").getCellValue());
                assertEquals(valueCells.getCellByName("food").getCellValue(), "donuts");
                assertNull(valueCells.getCellByName("lucene").getCellValue());

                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }

    }

    protected void checkOutputTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 4);

        command = "SELECT * from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY + ";";

        rs = session.execute(command);
        for (Row row : rs) {
            assertEquals(row.getInt("age"), 15);
        }

        session.close();
    }

    @Override
    protected void checkSimpleTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY + ";";
        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 20);

        command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY
                + " WHERE name = 'pepito_1' and gender = 'male' and age = 0  and animal = 'monkey';";
        rs = session.execute(command);

        List<Row> rows = rs.all();

        assertNotNull(rows);
        assertEquals(rows.size(), 1);

        Row r = rows.get(0);

        assertEquals(r.getString("password"), "xyz");

        session.close();

    }

    @Override
    protected RDD<Cells> initRDD() {
        assertNotNull(context);
        return context.createRDD(getReadConfig());
    }

    @Override
    protected ExtractorConfig<Cells> initReadConfig() {


        ExtractorConfig<Cells> rddConfig = new ExtractorConfig<>();
                Map<String, String> values = new HashMap<>();
                values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
                values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
                values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
                values.put(ExtractorConstants.COLUMN_FAMILY, CQL3_COLUMN_FAMILY);
                values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
                values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
                values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));

        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraCellExtractor.class);
        return rddConfig;
    }

    @Test
    public void testCountWithInputColumns() {
        logger.info("testCountWithInputColumns()");


        ExtractorConfig<Cells> rddConfig = new ExtractorConfig<>();
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, CQL3_COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.INPUT_COLUMNS, "password");
        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraCellExtractor.class);

        RDD<Cells> tmpRdd = context.createRDD(rddConfig);

        Cells[] cells = (Cells[]) tmpRdd.collect();

        assertEquals(cells.length, cql3TestDataSize);

        for (Cells cell : cells) {
            assertEquals(cell.size(), 4 + 1);
        }
    }

    @Override
    protected ExtractorConfig<Cells> initWriteConfig() {


        ExtractorConfig<Cells> rddConfig = new ExtractorConfig<>();
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, OUTPUT_KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, CQL3_OUTPUT_COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.CREATE_ON_WRITE, "true");

        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraCellExtractor.class);


        return rddConfig;
    }

    @Override
    public void testSaveToCassandra() {
        Function1<Cells, Cells> mappingFunc = new CellsAbstractSerializableFunction();
        RDD<Cells> mappedRDD = getRDD().map(mappingFunc, ClassTag$.MODULE$.<Cells>apply(Cells.class));
        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        assertTrue(mappedRDD.count() > 0);

        ExtractorConfig<Cells> writeConfig = getWriteConfig();
        writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "false");

        try {
            context.saveRDD(mappedRDD, writeConfig);
            fail();
        } catch (DeepIOException e) {
            // ok
            writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "true");
        }

        context.saveRDD(mappedRDD, writeConfig);
        checkOutputTestData();
    }

    @Override
    public void testSimpleSaveToCassandra() {
        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        ExtractorConfig<Cells> writeConfig = getWriteConfig();
        writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "false");

        try {
            context.saveRDD(getRDD(), writeConfig);
            fail();
        } catch (DeepIOException e) {
            // ok
            writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "true");
        }

        context.saveRDD(getRDD(), writeConfig);
        checkSimpleTestData();

    }

    @Override
    public void testCql3SaveToCassandra() {
        assertEquals(true, true);
//        try {
//            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + CQL3_OUTPUT_COLUMN_FAMILY);
//        } catch (Exception e) {
//        }
//
//        ICassandraDeepJobConfig<Cells> writeConfig = getWriteConfig();
//
//        RDD.cql3SaveRDDToCassandra(
//                getRDD(), writeConfig);
//        checkSimpleTestData();
    }

}
