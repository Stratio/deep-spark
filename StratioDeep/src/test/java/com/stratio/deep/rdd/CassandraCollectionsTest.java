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

import com.google.common.io.Resources;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.entity.Cql3CollectionsTestEntity;
import com.stratio.deep.util.Constants;
import com.stratio.deep.util.Utils;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(suiteName = "cassandraRddTests", dependsOnGroups = "ScalaCassandraEntityRDDTest", groups = "CassandraCollectionsTest" )
public class CassandraCollectionsTest extends CassandraRDDTest<Cql3CollectionsTestEntity> {

    private Logger logger = Logger.getLogger(getClass());

    @BeforeClass
    protected void initServerAndRDD() throws IOException, URISyntaxException, ConfigurationException,
        InterruptedException {
        super.initServerAndRDD();

        URL cql3TestData = Resources.getResource("cql3_collections_test_data.csv");

        String batch = "BEGIN BATCH \n";
        java.util.List<String> inserts = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
            cql3TestData.toURI()))))) {
            String line;

            String rawInsert = "INSERT INTO %s "
                + "(id, first_name, last_name, emails) VALUES (%s,%s,%s,%s);\n";

            int idx = 0;
            while ((line = br.readLine()) != null) {
                String[] fields = (CQL3_COLLECTION_COLUMN_FAMILY + "," + line).split(",");

                String emails = "{";

                for (int k = 3;  k<fields.length; k++){
                    if (k > 3){
                        emails += ",";
                    }
                    emails += Utils.singleQuote(fields[k]);
                }

                emails += "}";

                String insert = String.format(rawInsert, new Object[]{fields[0], fields[1], fields[2], emails});
                inserts.add(insert);
                ++idx;
            }

            logger.debug("idx: " + idx);
        } catch (Exception e) {
            logger.error("Error", e);
        }

        if (inserts.size() > 0) {
            for (String insert : inserts) {
                batch += insert;
            }
        }
        batch += " APPLY BATCH; ";

        executeCustomCQL(batch);
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
