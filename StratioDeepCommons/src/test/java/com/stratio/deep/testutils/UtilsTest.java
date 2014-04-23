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

package com.stratio.deep.testutils;

import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.testentity.TestEntity;
import com.stratio.deep.utils.AnnotationUtils;
import com.stratio.deep.utils.Utils;
import org.apache.cassandra.utils.Pair;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.*;

import static com.stratio.deep.utils.Utils.*;
import static org.testng.Assert.*;

public class UtilsTest {
    private static final String OUTPUT_KEYSPACE_NAME = "out_test_keyspace";
    private static final String OUTPUT_COLUMN_FAMILY = "out_test_page";


    class NotInstantiable implements IDeepType {

        private static final long serialVersionUID = -3311345712290429412L;
    }

    @Test
    public void testDeepType2Pair() {

        TestEntity te = new TestEntity();
        te.setDomain("abc.es");
        te.setId("43274632");
        te.setResponseCode(312);

        Tuple2<Cells, Cells> pair = deepType2tuple(te);

        assertNotNull(pair);
        assertNotNull(pair._1());
        assertNotNull(pair._2());
        assertEquals(pair._1().size(), 1);
        assertEquals(pair._2().size(), 8);

        assertEquals(pair._2().getCellByName("response_code").getCellValue(), 312);
        assertEquals(pair._2().getCellByName("domain_name").getCellValue(), "abc.es");

        assertEquals(pair._1().getCellByName("id").getCellValue(), "43274632");

    }

    @Test
    public void testFilterDeepFields() {
        Field[] fields = getAllFields(TestEntity.class);

        assertTrue(fields.length > 6);

        fields = AnnotationUtils.filterDeepFields(TestEntity.class);

        assertEquals(fields.length, 9);
    }

    @Test
    public void testFilterKeyFields() {
        Pair<Field[], Field[]> keyFields =
            AnnotationUtils.filterKeyFields(TestEntity.class);

        assertNotNull(keyFields);
        assertNotNull(keyFields.left);
        assertNotNull(keyFields.right);
        assertTrue(keyFields.left.length == 1);
        assertTrue(keyFields.right.length == 8);

        assertTrue(keyFields.left[0].getName().equals("id"));
    }

    @Test
    public void testAdditionalFilters() {
        assertEquals(additionalFilterGenerator(null), "");


        Map<String, Serializable> map = new TreeMap<>();
        assertEquals(additionalFilterGenerator(map), "");

        map.put("integer", 0L);
        assertEquals(additionalFilterGenerator(map), " AND \"integer\" = 0");
        map.remove("integer");

        map.put("lucene", null);

        assertEquals(additionalFilterGenerator(map), "");

        String filter = "address:* AND NOT address:*uropa*";

        map.put("lucene", filter);

        assertEquals(additionalFilterGenerator(map), " AND \"lucene\" = \'address:* AND NOT address:*uropa*\'");

        filter = "'address:* AND NOT address:*uropa*";

        map.put("lucene", filter);

        assertEquals(additionalFilterGenerator(map), " AND \"lucene\" = \'address:* AND NOT address:*uropa*\'");

        filter = "address:* AND NOT address:*uropa*'";

        map.put("lucene", filter);

        assertEquals(additionalFilterGenerator(map), " AND \"lucene\" = \'address:* AND NOT address:*uropa*\'");

        filter = "'address:* AND NOT address:*uropa*'";

        map.put("lucene", filter);

        assertEquals(additionalFilterGenerator(map), " AND \"lucene\" = \'address:* AND NOT address:*uropa*\'");
    }

    @Test
    public void testNewTypeInstance() {
        try {
            newTypeInstance(NotInstantiable.class);

            fail();
        } catch (DeepGenericException e) {
            // OK
        } catch (Exception e) {
            fail();
        }

        Utils.newTypeInstance(TestEntity.class);
    }

    @Test
    public void testUpdateQueryGenerator() {
        Cells keys = new Cells(Cell.create("id1", "", true, false), Cell.create("id2", "", true, false));

        Cells values = new Cells(Cell.create("domain_name", ""), Cell.create("url", ""), Cell.create("response_time",
            ""), Cell.create("response_code", ""), Cell.create("download_time", ""));

        String sql = updateQueryGenerator(keys, values, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);

        assertEquals(
            sql,
            "UPDATE "
                + OUTPUT_KEYSPACE_NAME
                + "."
                + OUTPUT_COLUMN_FAMILY
                + " SET \"domain_name\" = ?, \"url\" = ?, \"response_time\" = ?, \"response_code\" = ?, \"download_time\" = ? WHERE \"id1\" = ? AND \"id2\" = ?;"
        );

    }

    @Test
    public void testCreateTableQueryGeneratorComposite() {

        try {

            createTableQueryGenerator(null, null, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);
            fail();
        } catch (DeepGenericException e) {
            // ok
        }

        UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");

        Cells keys = new Cells(Cell.create("id1", "", true, false),
            Cell.create("id2", testTimeUUID, true, false),
            Cell.create("id3", new Integer(0), false, true));

        Cells values = new Cells(
            Cell.create("domain_name", ""),
            Cell.create("url", ""),
            Cell.create("response_time", new Long(0)),
            Cell.create("response_code", new Integer(200)),
            Cell.create("download_time", new Date()));

        String sql = createTableQueryGenerator(keys, values, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);

        assertEquals(sql,
            "CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
                " (\"id1\" text, \"id2\" timeuuid, \"id3\" int, \"domain_name\" text, \"url\" text, " +
                "\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
                "PRIMARY KEY ((\"id1\", \"id2\"), \"id3\"));"
        );


    }

    @Test
    public void testCreateTableQueryGeneratorSimple() {

        UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");

        Cells keys = new Cells(Cell.create("id1", testTimeUUID, true, false));

        Cells values = new Cells(
            Cell.create("domain_name", ""),
            Cell.create("url", ""),
            Cell.create("response_time", new Long(0)),
            Cell.create("response_code", new Integer(200)),
            Cell.create("download_time", new Date()));

        String sql = createTableQueryGenerator(keys, values, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);

        assertEquals(sql,
            "CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
                " (\"id1\" timeuuid, \"domain_name\" text, \"url\" text, " +
                "\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
                "PRIMARY KEY (\"id1\"));"
        );
    }

    @Test
    public void testPrepareTuple4CqlDriver() {
        UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");
        Date testDate = new Date();

        Cells keys = new Cells(Cell.create("id1", "", true, false),
            Cell.create("id2", testTimeUUID, true, false),
            Cell.create("id3", new Integer(0), false, true));

        Cells values = new Cells(
            Cell.create("domain_name", ""),
            Cell.create("url", ""),
            Cell.create("response_time", new Long(0)),
            Cell.create("response_code", new Integer(200)),
            Cell.create("download_time", testDate));

        Tuple2<String[], Object[]>
            bindVars = prepareTuple4CqlDriver(new Tuple2<Cells, Cells>(keys, values));

        String[] names = bindVars._1();
        Object[] vals = bindVars._2();

        assertEquals(names[0], "\"id1\"");
        assertEquals(names[1], "\"id2\"");
        assertEquals(names[2], "\"id3\"");
        assertEquals(names[3], "\"domain_name\"");
        assertEquals(names[4], "\"url\"");
        assertEquals(names[5], "\"response_time\"");
        assertEquals(names[6], "\"response_code\"");
        assertEquals(names[7], "\"download_time\"");

        assertEquals(vals[0], "");
        assertEquals(vals[1], testTimeUUID);
        assertEquals(vals[2], 0);
        assertEquals(vals[3], "");
        assertEquals(vals[4], "");
        assertEquals(vals[5], 0L);
        assertEquals(vals[6], 200);
        assertEquals(vals[7], testDate);
    }

    @Test
    public void testCellList2Tuple() {
        Cell domainName = Cell.create("domain_name", "");

        Cell id2 = Cell.create("id2", "", false, true);
        Cell responseTime = Cell.create("response_time", "");
        Cell url = Cell.create("url", "");
        Cell id1 = Cell.create("id1", "", true, false);
        Cell id3 = Cell.create("id3", "", true, false);
        Cell responseCode = Cell.create("response_code", "");
        Cell downloadTime = Cell.create("download_time", "");

        Cells cells = new Cells(
            domainName,
            id2,
            responseTime,
            url,
            id1,
            id3,
            responseCode,
            downloadTime);

        Tuple2<Cells, Cells> tuple = cellList2tuple(cells);
        assertNotNull(tuple);
        assertEquals(tuple._1().size(), 3);
        assertEquals(tuple._2().size(), 5);

        assertEquals(tuple._2().getCellByName("domain_name"), domainName);
        assertEquals(tuple._2().getCellByName("response_time"), responseTime);
        assertEquals(tuple._2().getCellByName("download_time"), downloadTime);
        assertEquals(tuple._2().getCellByName("response_code"), responseCode);

        assertEquals(tuple._1().getCellByName("id2"), id2);
        assertEquals(tuple._1().getCellByName("id1"), id1);
        assertEquals(tuple._1().getCellByName("id3"), id3);
    }

    @Test
    public void testSingleQuote() {
        assertEquals(singleQuote(""), "");
        assertEquals(singleQuote("test"), "'test'");

        assertEquals(singleQuote(" test "), "'test'");
        assertEquals(singleQuote(" 'test'       "), "'test'");
        assertEquals(singleQuote("'test''"), "'test''");
    }

    @Test
    public void testQuote() {
        assertEquals(quote(""), "");

        assertEquals(quote("test"), "\"test\"");

        assertEquals(quote(" test "), "\"test\"");
        assertEquals(quote(" \"test\"       "), "\"test\"");
        assertEquals(quote("'test''"), "\"'test''\"");
    }

    @Test
    public void testBatchQueryGenerator(){
        String stmt1 =  "CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
            " (\"id1\" timeuuid, \"domain_name\" text, \"url\" text, " +
            "\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
            "PRIMARY KEY (\"id1\"));";

        String stmt2 = "UPDATE "
            + OUTPUT_KEYSPACE_NAME
            + "."
            + OUTPUT_COLUMN_FAMILY
            + " SET \"domain_name\" = ?, \"url\" = ?, \"response_time\" = ?, \"response_code\" = ?, \"download_time\" = ? WHERE \"id1\" = ? AND \"id2\" = ?;";

        String stmt3 = "CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
            " (\"id1\" timeuuid, \"domain_name\" text, \"url\" text, " +
            "\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
            "PRIMARY KEY (\"id1\"));";

        List<String> stmts = Arrays.asList(stmt1, stmt2, stmt3);

        String batch = batchQueryGenerator(stmts);

        assertEquals(batch,
            "BEGIN BATCH \n"+stmt1+"\n"+stmt2+"\n"+stmt3+"\n"+" APPLY BATCH;");
    }

    @Test
    public void testFindSetter(){
        try {
            findSetter("not_existent_field", TestSetterClass.class, BigInteger.class);

            fail();
        } catch (DeepIOException e) {
            // ok;
        }

        try {
            findSetter("description", TestSetterClass.class, String.class);

            fail();
        } catch (DeepIOException e) {
            // ok;
        }

        assertNotNull(findSetter("id", TestSetterClass.class, Integer.class));
        assertNotNull(findSetter("uuid", TestSetterClass.class, UUID.class));
        assertNotNull(findSetter("scalalong", TestSetterClass.class, Long.class));
    }

    @Test
    public void testGetAllFields(){
        Field[] fields  = getAllFields(TestEntity.class);
        assertEquals(fields.length, 11);
    }

    class TestSetterClass implements IDeepType{
        private Integer id;
        private UUID uuid;
        private String description;

        private Long scalalong;

        public Long getScalalong() {
            return scalalong;
        }

        public void scalalong_$eq(Long scalalong){
            this.scalalong = scalalong;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public UUID getUuid() {
            return uuid;
        }

        public void setUuid(UUID uuid) {
            this.uuid = uuid;
        }

        public String getDescription() {
            return description;
        }
    }
}
