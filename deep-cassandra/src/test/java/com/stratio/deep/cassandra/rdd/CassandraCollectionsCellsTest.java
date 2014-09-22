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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.embedded.CassandraServer;
import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import com.stratio.deep.cassandra.testentity.Cql3CollectionsTestEntity;
import com.stratio.deep.commons.utils.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Function1;
import scala.reflect.ClassTag$;

import static org.testng.Assert.*;

/**
 * Integration tests for generic cell RDDs where cells contain Cassandra's collections.
 */
@Test(suiteName = "cassandraRddTests", dependsOnGroups = "CassandraCollectionsEntityTest",
        groups = "CassandraCollectionsCellsTest")
public class CassandraCollectionsCellsTest extends CassandraRDDTest<Cells> {
    @BeforeClass
    protected void initServerAndRDD() throws IOException, URISyntaxException, ConfigurationException,
            InterruptedException {
        super.initServerAndRDD();

        CassandraCollectionsEntityTest.loadCollectionsData();
    }

    @Override
    protected void checkComputedData(Cells[] entities) {

        boolean found = false;

        assertEquals(entities.length, 500);

	    String keyspace = getReadConfig().getValues().get(ExtractorConstants.KEYSPACE);

        for (Cells e : entities) {
            Integer id = (Integer) e.getCellByName("id").getCellValue();

            if (id.equals(470)) {
                String firstName = (String) e.getCellByName("first_name").getCellValue();
                String lastName = (String) e.getCellByName("last_name").getCellValue();

                Collection<String> emails = (Collection<String>) e.getCellByName("emails").getCellValue();
                Collection<String> phones = (Collection<String>) e.getCellByName("phones").getCellValue();
                Map<UUID, Integer> uuid2id = (Map<UUID, Integer>) e.getCellByName("uuid2id").getCellValue();

                assertEquals(firstName, "Amalda");
                assertEquals(lastName, "Banks");
                assertNotNull(emails);
                assertEquals(emails.size(), 2);
                assertEquals(phones.size(), 2);
                assertEquals(uuid2id.size(), 1);
                assertEquals(uuid2id.get(UUID.fromString("AE47FBFD-A086-47C2-8C73-77D8A8E99F35")),
                        Integer.valueOf(470));

                Iterator<String> emailsIter = emails.iterator();
                Iterator<String> phonesIter = phones.iterator();

                assertEquals(emailsIter.next(), "AmaldaBanks@teleworm.us");
                assertEquals(emailsIter.next(), "MarcioColungaPichardo@dayrep.com");

                assertEquals(phonesIter.next(), "801-527-1039");
                assertEquals(phonesIter.next(), "925-348-9339");
                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }
    }

    @Override
    protected void checkSimpleTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 500);

        command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + " WHERE \"id\" = 351;";

        rs = session.execute(command);
        Row row = rs.one();

        String firstName = row.getString("first_name");
        String lastName = row.getString("last_name");
        Set<String> emails = row.getSet("emails", String.class);
        List<String> phones = row.getList("phones", String.class);
        Map<UUID, Integer> uuid2id = row.getMap("uuid2id", UUID.class, Integer.class);

        assertEquals(firstName, "Gustava");
        assertEquals(lastName, "Palerma");
        assertNotNull(emails);
        assertEquals(emails.size(), 2);

        assertNotNull(phones);
        assertEquals(phones.size(), 2);

        assertNotNull(uuid2id);
        assertEquals(uuid2id.size(), 1);
        assertEquals(uuid2id.get(UUID.fromString("BAB7F03E-0D9F-4466-BD8A-5F7373802610")).intValue(), 351);

        session.close();
    }

    @Override
    protected RDD<Cells> initRDD() {
        return context.createRDD(getReadConfig());
    }

    @Override
    protected ExtractorConfig<Cells> initReadConfig() {

        ExtractorConfig<Cells> rddConfig = new ExtractorConfig<>();
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, CQL3_COLLECTION_COLUMN_FAMILY);
        values.put(ExtractorConstants.PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));

        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraCellExtractor.class);
        return rddConfig;

    }

    @Override
    protected ExtractorConfig<Cells> initWriteConfig() {

        ExtractorConfig<Cells> rddConfig = new ExtractorConfig<>();
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, OUTPUT_KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
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
        Function1<Cells, Cells> mappingFunc =
                new TestEntityAbstractSerializableFunction();

        RDD<Cells> mappedRDD =
                getRDD().map(mappingFunc, ClassTag$.MODULE$.<Cells>apply(Cql3CollectionsTestEntity.class));

        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
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

    protected void checkOutputTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 500);

        command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + " WHERE \"id\" = 351;";

        rs = session.execute(command);
        Row row = rs.one();

        String firstName = row.getString("first_name");
        String lastName = row.getString("last_name");
        Set<String> emails = row.getSet("emails", String.class);
        List<String> phones = row.getList("phones", String.class);
        Map<UUID, Integer> uuid2id = row.getMap("uuid2id", UUID.class, Integer.class);

        assertEquals(firstName, "Gustava_out");
        assertEquals(lastName, "Palerma_out");
        assertNotNull(emails);
        assertEquals(emails.size(), 3);
        assertTrue(emails.contains("klv@email.com"));

        assertNotNull(phones);
        assertEquals(phones.size(), 3);
        assertTrue(phones.contains("111-111-1111112"));

        assertNotNull(uuid2id);
        assertEquals(uuid2id.size(), 1);
        assertEquals(uuid2id.get(UUID.fromString("BAB7F03E-0D9F-4466-BD8A-5F7373802610")).intValue() - 10, 351);

        session.close();
    }

    @Override
    public void testSimpleSaveToCassandra() {
        ExtractorConfig<Cells> writeConfig = getWriteConfig();
        writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "false");

        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        try {
            context.saveRDD(getRDD(), writeConfig);

            fail();
        } catch (Exception e) {
            // ok
            writeConfig.putValue(ExtractorConstants.CREATE_ON_WRITE, "true");
        }

        context.saveRDD(getRDD(), writeConfig);

        checkSimpleTestData();
    }

    @Override
    public void testCql3SaveToCassandra() {
        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        ExtractorConfig<Cells> writeConfig = getWriteConfig();

//        RDD.cql3SaveRDDToCassandra(getRDD(), writeConfig);
        checkSimpleTestData();
    }

    private static class TestEntityAbstractSerializableFunction extends
            AbstractSerializableFunction<Cells, Cells> {

        private static final long serialVersionUID = -1555102599662015841L;

        @Override
        public Cells apply(Cells e) {
            Cell id = e.getCellByName("id");
            Cell fn = e.getCellByName("first_name");
            Cell ln = e.getCellByName("last_name");
            Cell em = e.getCellByName("emails");
            Cell ph = e.getCellByName("phones");
            Cell uu = e.getCellByName("uuid2id");

            Cell newid = CassandraCell.create(id, id.getCellValue());

            Cell newfn = CassandraCell.create(fn, fn.getCellValue() + "_out");

            Cell newln = CassandraCell.create(ln, ln.getCellValue() + "_out");

            Set<String> emails = (Set<String>) em.getCellValue();
            emails.add("klv@email.com");

            Cell newem = CassandraCell.create(em, emails);

            List<String> phones = (List<String>) ph.getCellValue();
            phones.add("111-111-1111112");

            Cell newph = CassandraCell.create(ph, phones);

            Map<UUID, Integer> uuid2id = (Map<UUID, Integer>) uu.getCellValue();
            for (Map.Entry<UUID, Integer> entry : uuid2id.entrySet()) {
                entry.setValue(entry.getValue() + 10);
            }
            Cell newuu = CassandraCell.create(uu, uuid2id);

            return new Cells(e.getDefaultTableName(), newid, newfn, newln, newem, newph, newuu);
        }
    }

}
