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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.embedded.CassandraServer;
import com.stratio.deep.cassandra.extractor.CassandraEntityExtractor;
import com.stratio.deep.cassandra.testentity.Cql3CollectionsTestEntity;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.exception.DeepIndexNotFoundException;
import com.stratio.deep.commons.exception.DeepNoSuchFieldException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import com.stratio.deep.cassandra.testentity.TestEntity;
import com.stratio.deep.commons.utils.Constants;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.testng.annotations.Test;
import scala.Function1;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import static com.stratio.deep.commons.utils.Utils.quote;
import static org.testng.Assert.*;

/**
 * Integration tests for entity RDDs.
 */
@Test(suiteName = "cassandraRddTests", groups = {"CassandraEntityRDDTest"})
public class CassandraEntityRDDTest extends CassandraRDDTest<TestEntity> {
    private Logger logger = Logger.getLogger(CassandraEntityRDDTest.class);

    private static class TestEntityAbstractSerializableFunction extends
            AbstractSerializableFunction<TestEntity, TestEntity> {

        private static final long serialVersionUID = -1555102599662015841L;

        @Override
        public TestEntity apply(TestEntity e) {
            return new TestEntity(e.getId(), e.getDomain(), e.getUrl(), e.getResponseTime() + 1, e.getResponseCode(),
                    e.getNotMappedField());
        }
    }

    @Override
    protected void checkComputedData(TestEntity[] entities) {
        boolean found = false;

        assertEquals(entities.length, entityTestDataSize);

        for (TestEntity e : entities) {
            if (e.getId().equals("e71aa3103bb4a63b9e7d3aa081c1dc5ddef85fa7")) {
                assertEquals(e.getUrl(), "http://11870.com/k/es/de");
                assertEquals(e.getResponseTime(), new Integer(421));
                assertEquals(e.getDownloadTime(), new Long(1380802049275L));
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

        String command = "select count(*) from " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY) + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), entityTestDataSize);

        command = "SELECT * from " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY)
                + " WHERE \"id\" = 'e71aa3103bb4a63b9e7d3aa081c1dc5ddef85fa7';";

        rs = session.execute(command);
        Row row = rs.one();

        assertEquals(row.getString("domain_name"), "11870.com");
        assertEquals(row.getString("url"), "http://11870.com/k/es/de");
        assertEquals(row.getInt("response_time"), 421 + 1);

        //cannot delete a column using CQL, forcing it to null converts it to 0!!! see CASSANDRA-5885 and CASSANDRA-6180
        assertEquals(row.getLong("download_time"), 0);
        session.close();
    }

    @Override
    protected void checkSimpleTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY) + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), entityTestDataSize);

        command = "select * from " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY)
                + " WHERE \"id\" = 'e71aa3103bb4a63b9e7d3aa081c1dc5ddef85fa7';";

        rs = session.execute(command);
        Row row = rs.one();

        assertEquals(row.getString("domain_name"), "11870.com");
        assertEquals(row.getInt("response_time"), 421);
        assertEquals(row.getLong("download_time"), 1380802049275L);
        assertEquals(row.getString("url"), "http://11870.com/k/es/de");
        session.close();
    }

    @Test
    public void testAdditionalFilters() {


        try {
            CassandraConfigFactory
                    .create(TestEntity.class)
                    .host(Constants.DEFAULT_CASSANDRA_HOST)
                    .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                    .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                    .keyspace(KEYSPACE_NAME)
                    .columnFamily(COLUMN_FAMILY)
                    .filterByField("notExistentField", "val")
                    .initialize();

            fail();
        } catch (DeepNoSuchFieldException e) {
            // OK
        }

        try {
            CassandraConfigFactory
                    .create(TestEntity.class)
                    .host(Constants.DEFAULT_CASSANDRA_HOST)
                    .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                    .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                    .keyspace(KEYSPACE_NAME)
                    .columnFamily(COLUMN_FAMILY)
                    .filterByField("url", "val")
                    .initialize();

            fail();
        } catch (DeepIndexNotFoundException e) {
            // OK
        }

        TestEntity[] entities = (TestEntity[]) rdd.collect();
        int allElements = entities.length;
        assertTrue(allElements > 2);



        ExtractorConfig<TestEntity> rddConfig = new ExtractorConfig<>(TestEntity.class);
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.FILTER_QUERY, "response_time,371");
        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraEntityExtractor.class);

        RDD<TestEntity> otherRDD = context.createRDD(rddConfig);

        entities = (TestEntity[]) otherRDD.collect();
        assertEquals(entities.length, 2);

        /*
        config = DeepJobConfigFactory
                .create(TestEntity.class)
                .host(Constants.DEFAULT_CASSANDRA_HOST)
                .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                .keyspace(KEYSPACE_NAME)
                .columnFamily(COLUMN_FAMILY)
                .filterByField("lucene", "{filter:{type:\"range\",field:\"response_time\",lower:160,upper:840," +
                        "include_lower:true,include_upper:true}}")
                .initialize();

        otherRDD = context.cassandraEntityRDD(config);

        entities = (TestEntity[]) otherRDD.collect();
        assertEquals(entities.length, 9);
        */
    }

    @Override
    protected RDD<TestEntity> initRDD() {
        assertNotNull(context);
        return (RDD)context.createRDD(getReadConfig());
    }

    @Override
    protected ExtractorConfig<TestEntity> initReadConfig() {

        ExtractorConfig<TestEntity> rddConfig = new ExtractorConfig<>(TestEntity.class);
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraEntityExtractor.class);
        return rddConfig;
    }

    @Override
    protected ExtractorConfig<TestEntity> initWriteConfig() {

        ExtractorConfig<TestEntity> rddConfig = new ExtractorConfig<>(TestEntity.class);
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, OUTPUT_KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, OUTPUT_COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.CREATE_ON_WRITE, "true");

        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraEntityExtractor.class);

        return rddConfig;

    }

    @Test
    public void testCountWithInputColumns() {
        logger.info("testCountWithInputColumns()");

        ExtractorConfig<TestEntity> rddConfig = new ExtractorConfig<>(TestEntity.class);
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(2));
        values.put(ExtractorConstants.INPUT_COLUMNS, "domain_name, response_time");
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));

        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraEntityExtractor.class);

        RDD<TestEntity> tmpRdd = context.createRDD(rddConfig);

        TestEntity[] cells = (TestEntity[]) tmpRdd.collect();

        assertEquals(cells.length, entityTestDataSize);

        for (TestEntity e : cells) {
            assertNotNull(e.getDomain());
            assertNotNull(e.getResponseTime());
            assertNotNull(e.getId());

            assertNull(e.getResponseCode());
            assertNull(e.getDownloadTime());
            assertNull(e.getNotMappedField());
            assertNull(e.getUrl());
        }
    }

    @Override
    public void testSaveToCassandra() {
        Function1<TestEntity, TestEntity> mappingFunc = new TestEntityAbstractSerializableFunction();

        RDD<TestEntity> mappedRDD = getRDD().map(mappingFunc, ClassTag$.MODULE$.<TestEntity>apply(TestEntity.class));

        try {
            executeCustomCQL("DROP TABLE " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY));
        } catch (Exception e) {
        }

        assertTrue(mappedRDD.count() > 0);

        ExtractorConfig<TestEntity> writeConfig = getWriteConfig();
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
        ExtractorConfig<TestEntity> writeConfig = getWriteConfig();
        writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "false");

        try {
	        executeCustomCQL("DROP TABLE " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY));
        } catch (Exception e) {
        }

        try {
            context.saveRDD(getRDD(), writeConfig);

            fail();
        } catch (Exception e) {
            // ok
            writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "true");
        }

        assertEquals(getRDD().count(), entityTestDataSize);
        context.saveRDD(getRDD(), writeConfig);

        checkSimpleTestData();
    }

    @Override
    public void testCql3SaveToCassandra() {

        try {
	        executeCustomCQL("DROP TABLE " + quote(OUTPUT_KEYSPACE_NAME) + "." + quote(OUTPUT_COLUMN_FAMILY));
        } catch (Exception e) {
        }

        ExtractorConfig<TestEntity> writeConfig = getWriteConfig();

//        RDD.cql3SaveRDDToCassandra(getRDD(), writeConfig);
        checkSimpleTestData();
    }


    @Test
    public void testJavaSerialization() {
        JavaSerializer ser = new JavaSerializer(context.getConf());

        SerializerInstance instance = ser.newInstance();
        ClassTag<RDD<TestEntity>> classTag = ClassTag$.MODULE$.<RDD<TestEntity>>apply(rdd.getClass());

        ByteBuffer serializedRDD = instance.serialize(rdd, classTag);

        RDD deserializedRDD = instance.deserialize(serializedRDD, classTag);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        SerializationStream serializationStream = instance.serializeStream(baos);
        serializationStream = serializationStream.writeObject(rdd, classTag);

        serializationStream.flush();
        serializationStream.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(serializedRDD.array());

        DeserializationStream deserializationStream = instance.deserializeStream(bais);
        Iterator<Object> iter = deserializationStream.asIterator();
        assertTrue(iter.hasNext());

        deserializedRDD = (RDD) iter.next();
        assertNotNull(deserializedRDD);
    }

}
