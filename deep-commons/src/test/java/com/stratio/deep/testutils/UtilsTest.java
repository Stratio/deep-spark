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

import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.testentity.CommonsTestEntity;
import com.stratio.deep.utils.AnnotationUtils;
import com.stratio.deep.utils.Pair;
import com.stratio.deep.utils.Utils;
import org.testng.annotations.Test;

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
    public void testFilterDeepFields() {
        Field[] fields = getAllFields(CommonsTestEntity.class);

        assertTrue(fields.length > 6);

        fields = AnnotationUtils.filterDeepFields(CommonsTestEntity.class);

        assertEquals(fields.length, 9);
    }

    @Test
    public void testFilterKeyFields() {
        Pair<Field[], Field[]> keyFields =
                AnnotationUtils.filterKeyFields(CommonsTestEntity.class);

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

        Utils.newTypeInstance(CommonsTestEntity.class);
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
    public void testBatchQueryGenerator() {
        String stmt1 = "CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
                " (\"id1\" timeuuid, \"domain_name\" text, \"url\" text, " +
                "\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
                "PRIMARY KEY (\"id1\"));";

        String stmt2 = "UPDATE "
                + OUTPUT_KEYSPACE_NAME
                + "."
                + OUTPUT_COLUMN_FAMILY
                + " SET \"domain_name\" = ?, \"url\" = ?, \"response_time\" = ?, \"response_code\" = ?, " +
                "\"download_time\" = ? WHERE \"id1\" = ? AND \"id2\" = ?;";

        String stmt3 = "CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
                " (\"id1\" timeuuid, \"domain_name\" text, \"url\" text, " +
                "\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
                "PRIMARY KEY (\"id1\"));";

        List<String> stmts = Arrays.asList(stmt1, stmt2, stmt3);

        String batch = batchQueryGenerator(stmts);

        assertEquals(batch,
                "BEGIN BATCH \n" + stmt1 + "\n" + stmt2 + "\n" + stmt3 + "\n" + " APPLY BATCH;");
    }

    @Test
    public void testFindSetter() {
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
    public void testGetAllFields() {
        Field[] fields = getAllFields(CommonsTestEntity.class);
        assertTrue(fields.length >= 11);
    }

    class TestSetterClass implements IDeepType {
        private Integer id;
        private UUID uuid;
        private String description;

        private Long scalalong;

        public Long getScalalong() {
            return scalalong;
        }

        public void scalalong_$eq(Long scalalong) {
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
