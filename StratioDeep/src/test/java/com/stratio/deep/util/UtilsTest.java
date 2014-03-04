package com.stratio.deep.util;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.entity.TestEntity;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.utils.AnnotationUtils;
import org.apache.cassandra.utils.Pair;
import org.testng.annotations.Test;
import scala.Tuple2;

import static com.stratio.deep.context.AbstractDeepSparkContextTest.OUTPUT_COLUMN_FAMILY;
import static com.stratio.deep.context.AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME;
import static com.stratio.deep.util.Utils.*;
import static org.testng.Assert.*;

public class UtilsTest {
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
        assertEquals(pair._2().size(), 5);

        assertEquals(pair._2().getCellByName("response_code").getCellValue(), 312);
        assertEquals(pair._2().getCellByName("domain_name").getCellValue(), "abc.es");

        assertEquals(pair._1().getCellByName("id").getCellValue(), "43274632");

    }

    @Test
    public void testFilterDeepFields() {
        Field[] fields = TestEntity.class.getDeclaredFields();

        assertTrue(fields.length > 6);

        fields = AnnotationUtils.filterDeepFields(fields);

        assertEquals(fields.length, 6);
    }

    @Test
    public void testFilterKeyFields() {
        Field[] fields = TestEntity.class.getDeclaredFields();

        Pair<Field[], Field[]> keyFields = AnnotationUtils.filterKeyFields(AnnotationUtils.filterDeepFields(fields));

        assertNotNull(keyFields);
        assertNotNull(keyFields.left);
        assertNotNull(keyFields.right);
        assertTrue(keyFields.left.length == 1);
        assertTrue(keyFields.right.length == 5);

        assertTrue(keyFields.left[0].getName().equals("id"));
    }

    @Test
    public void testAdditionalFilters() {
        Map<String, Serializable> map = new TreeMap<>();

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

        filter = "'address:* AND NOT address:\"*uropa*\"'";

        map.put("lucene", filter);

        try {
            additionalFilterGenerator(map);

            // if there's no error the code is broken
            fail();
        } catch (DeepGenericException e) {
            // ok
        }
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
                + " SET \"domain_name\" = ?, \"url\" = ?, \"response_time\" = ?, \"response_code\" = ?, \"download_time\" = ? WHERE \"id1\" = ? AND \"id2\" = ?;");

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
                "PRIMARY KEY ((\"id1\", \"id2\"), \"id3\"));");


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
                "PRIMARY KEY (\"id1\"));");


    }
}
