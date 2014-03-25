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

package com.stratio.deep.rdd;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.google.common.io.Resources;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.entity.Cql3CollectionsTestEntity;
import com.stratio.deep.util.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(enabled = false, suiteName = "cassandraRddTests", dependsOnGroups = "ScalaCassandraEntityRDDTest", groups = "CassandraCollectionsTest" )
public class CassandraCollectionsTest extends CassandraRDDTest<Cql3CollectionsTestEntity> {

    private Logger logger = Logger.getLogger(getClass());

    @BeforeClass
    protected void initServerAndRDD() throws IOException, URISyntaxException, ConfigurationException,
        InterruptedException {
        super.initServerAndRDD();

        URL cql3TestData = Resources.getResource("cql3_collections_test_data.csv");

        Batch batch = QueryBuilder.batch();
        java.util.List<Statement> inserts = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
            cql3TestData.toURI()))))) {
            String line;

            int idx = 0;
            final int emailsStartPos = 3;

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");

                Set<String> emails = new HashSet<>();

                for (int k = emailsStartPos;  k<fields.length; k++){
                    emails.add(fields[k]);
                }

                Insert stmt = QueryBuilder.insertInto(CQL3_COLLECTION_COLUMN_FAMILY).values(
                    new String[]{"id", "first_name", "last_name", "emails"},
                    new Object[]{Integer.parseInt(fields[0]), fields[1], fields[2], emails});

                batch.add(stmt);
                ++idx;
            }

            logger.debug("idx: " + idx);
        } catch (Exception e) {
            logger.error("Error", e);
        }


        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
            .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect(KEYSPACE_NAME);
        session.execute(batch);
    }

    @Override
    protected void checkComputedData(Cql3CollectionsTestEntity[] entities) {

        /*
        boolean found = false;

        assertEquals(entities.length, 500);

        for (Cql3CollectionsTestEntity e : entities) {
            if (e.getId().equals(470)) {
                assertEquals(e.getFirstName(), "Amalda");
                assertEquals(e.getLastName(), "Banks");
                assertNotNull(e.getEmails());
                assertEquals(e.getEmails().size(), 2);

                Iterator<String> emails = e.getEmails().iterator();

                assertEquals(emails.next(), "AmaldaBanks@teleworm.us");
                assertEquals(emails.next(), "MarcioColungaPichardo@dayrep.com");

                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }
        */
    }

    @Override
    protected void checkSimpleTestData() {

    }

    @Override
    protected CassandraRDD<Cql3CollectionsTestEntity> initRDD() {
        return context.cassandraEntityRDD(getReadConfig());
    }

    @Override
    protected IDeepJobConfig<Cql3CollectionsTestEntity> initReadConfig() {
        IDeepJobConfig<Cql3CollectionsTestEntity> config = DeepJobConfigFactory.create(Cql3CollectionsTestEntity.class)
            .host(Constants.DEFAULT_CASSANDRA_HOST).rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
            .cqlPort(CassandraServer.CASSANDRA_CQL_PORT).keyspace(KEYSPACE_NAME).columnFamily(CQL3_COLLECTION_COLUMN_FAMILY);

        return config.initialize();
    }

    @Override
    protected IDeepJobConfig<Cql3CollectionsTestEntity> initWriteConfig() {
        return null;
    }

    @Override
    public void testSaveToCassandra() {

    }

    @Override
    public void testSimpleSaveToCassandra() {

    }

    @Override
    public void testCql3SaveToCassandra() {

    }
}
