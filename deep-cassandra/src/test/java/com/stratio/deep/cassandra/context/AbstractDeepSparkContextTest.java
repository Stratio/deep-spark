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

package com.stratio.deep.cassandra.context;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.google.common.io.Resources;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.cassandra.embedded.CassandraServer;
import com.stratio.deep.commons.utils.Constants;
import com.stratio.deep.core.context.DeepSparkContext;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import static com.stratio.deep.commons.utils.Utils.quote;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Base class for all Deep's integration tests.
 */
public abstract class AbstractDeepSparkContextTest {

    private Logger logger = Logger.getLogger(getClass());
    protected static DeepSparkContext context;

    private static CassandraServer cassandraServer;
    public static final String KEYSPACE_NAME = "Test_Keyspace";
    public static final String COLUMN_FAMILY = "test_Page";
    public static final String OUTPUT_KEYSPACE_NAME = "out_test_keyspace";
    public static final String OUTPUT_COLUMN_FAMILY = "Out_Test_Page";
    public static final String CQL3_COLUMN_FAMILY = "cql3_cf";
    public static final String CQL3_OUTPUT_COLUMN_FAMILY = "cql3_output_cf";
    public static final String CQL3_COLLECTION_COLUMN_FAMILY = "cql3_collection_cf";
    public static final String OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY = "cql3_collection_output_cf";

    public static final String CQL3_ENTITY_OUTPUT_COLUMN_FAMILY = "cql3_entity_output_cf";

    protected static final int entityTestDataSize = 19;
    protected static final int cql3TestDataSize = 20;

    protected String createCF = "CREATE TABLE " + quote(KEYSPACE_NAME) + "." + quote(COLUMN_FAMILY) + " (id text PRIMARY " +
            "KEY, " + "url text, "
            + "domain_name text, " + "response_code int, " + "charset text," + "response_time int,"
            + "download_time bigint," + "first_download_time bigint," + "title text, lucene text ) ;";

    /*
    protected String createLuceneIndex =
            "CREATE CUSTOM INDEX page_lucene ON " + KEYSPACE_NAME + "." + quote(COLUMN_FAMILY) + " (lucene) USING " +
                    "'org.apache.cassandra.db.index.stratio.RowIndex' " +
                    "WITH OPTIONS = {'refresh_seconds':'1', 'schema':'{default_analyzer:\"org.apache.lucene.analysis" +
                    ".standard.StandardAnalyzer\", " +
                    "fields:{ charset:{type:\"string\"}, url:{type:\"string\"}, domain_name:{type:\"string\"}, " +
                    "response_code:{type:\"integer\"}, id:{type:\"string\"}, response_time:{type:\"integer\"} } }'};";
    */

    protected String createCFIndex = "create index idx_" + COLUMN_FAMILY + "_resp_time on " + quote(KEYSPACE_NAME) + "." +
            quote(COLUMN_FAMILY) + " (response_time);";

    protected String createCql3CF = "create table " + quote(KEYSPACE_NAME) + "." + CQL3_COLUMN_FAMILY
            + "(name varchar, password varchar, color varchar, gender varchar, food varchar, "
            + " animal varchar, lucene varchar,age int,PRIMARY KEY ((name, gender), age, animal)); ";

    protected String createCql3CFIndex = "create index idx_" + CQL3_COLUMN_FAMILY + "_food on " + quote(KEYSPACE_NAME) + "."
            + CQL3_COLUMN_FAMILY + "(food);";

    protected String createCql3CollectionsCF =
            "CREATE TABLE " + quote(KEYSPACE_NAME) + "." + CQL3_COLLECTION_COLUMN_FAMILY +
                    " ( id int PRIMARY KEY, first_name text, last_name text, emails set<text>, phones list<text>, " +
                    "uuid2id map<uuid,int>);";

    protected String buildTestDataInsertBatch() {
        URL testData = Resources.getResource("testdata.csv");
        URL cql3TestData = Resources.getResource("cql3_test_data.csv");

        String batch = "BEGIN BATCH \n";
        java.util.List<String> inserts = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
                new File(testData.toURI()))))) {
            String line;

            String rawInsert = "INSERT INTO %s (" + "\"id\", \"charset\", \"domain_name\", "
                    + "\"download_time\", \"response_time\", " + "\"first_download_time\", \"url\") "
                    + "values (\'%s\', \'%s\', \'%s\', %s, %s, %s, \'%s\');";

            while ((line = br.readLine()) != null) {
                String[] fields = (quote(COLUMN_FAMILY) + "," + line).split(",");
                String insert = String.format(rawInsert, (Object[]) fields);
                inserts.add(insert);

            }
        } catch (Exception e) {
            logger.error("Error", e);
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
                cql3TestData.toURI()))))) {
            String line;

            String rawInsert = "INSERT INTO %s "
                    + "(name, gender, age, animal, food, password) VALUES (%s,%s,%s,%s,%s,%s);\n";

            int idx = 0;
            while ((line = br.readLine()) != null) {
                String[] fields = (CQL3_COLUMN_FAMILY + "," + line).split(",");
                String insert = String.format(rawInsert, (Object[]) fields);
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

        return batch;
    }

    private void checkTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();

        Session session = cluster.connect();

        String command = "select count(*) from " + quote(KEYSPACE_NAME) + "." + quote(COLUMN_FAMILY) + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), entityTestDataSize);

        command = "select * from " + quote(KEYSPACE_NAME) + "." + quote(COLUMN_FAMILY)
                + " WHERE \"id\" = 'e71aa3103bb4a63b9e7d3aa081c1dc5ddef85fa7';";

        rs = session.execute(command);
        Row row = rs.one();

        assertEquals(row.getString("domain_name"), "11870.com");
        assertEquals(row.getInt("response_time"), 421);
        assertEquals(row.getLong("download_time"), 1380802049275L);
        assertEquals(row.getString("charset"), "UTF-8");
        assertEquals(row.getLong("first_download_time"), 1380802049276L);
        assertEquals(row.getString("url"), "http://11870.com/k/es/de");

        command = "select count(*) from " + quote(KEYSPACE_NAME) + "." + CQL3_COLUMN_FAMILY + ";";
        rs = session.execute(command);
        assertEquals(rs.one().getLong(0), cql3TestDataSize);

        command = "select * from " + quote(KEYSPACE_NAME) + "." + CQL3_COLUMN_FAMILY
                + " WHERE name = 'pepito_3' and gender = 'male' and age = -2 and animal = 'monkey';";

        rs = session.execute(command);

        List<Row> rows = rs.all();

        assertNotNull(rows);
        assertEquals(rows.size(), 1);

        Row r = rows.get(0);

        assertEquals(r.getString("password"), "abc");
        assertEquals(r.getString("food"), "donuts");

        session.close();
    }

    @AfterSuite
    protected void disposeServerAndRdd() throws IOException {
        if (cassandraServer != null) {
            cassandraServer.shutdown();
        }

        if (context != null) {
            context.stop();
        }
    }

    protected static void executeCustomCQL(String... cqls) {

        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();
        for (String cql : cqls) {
            session.execute(cql);
        }
        session.close();
    }

    @BeforeSuite
    protected void initContextAndServer() throws ConfigurationException, IOException, InterruptedException {
        if (context==null && cassandraServer==null) {

            logger.info("instantiating context");
            context = new DeepSparkContext("local", "deepSparkContextTest");

            String createKeyspace = "CREATE KEYSPACE " + quote(KEYSPACE_NAME)
                    + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";

            String createOutputKeyspace = "CREATE KEYSPACE " + OUTPUT_KEYSPACE_NAME
                    + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";

            String useKeyspace = "USE " + quote(KEYSPACE_NAME) + ";";

            String useOutputKeyspace = "USE " + OUTPUT_KEYSPACE_NAME + ";";

            String initialDataset = buildTestDataInsertBatch();

            String[] startupCommands = new String[]{createKeyspace, createOutputKeyspace, useKeyspace, createCF,
                    createCFIndex, /*createLuceneIndex,*/
                    createCql3CF, createCql3CFIndex, createCql3CollectionsCF, initialDataset, useOutputKeyspace};

            cassandraServer = new CassandraServer();
            cassandraServer.setStartupCommands(startupCommands);
            cassandraServer.start();
           checkTestData();
        }
    }

    protected DeepSparkContext getContext() {
        return context;
    }

}
