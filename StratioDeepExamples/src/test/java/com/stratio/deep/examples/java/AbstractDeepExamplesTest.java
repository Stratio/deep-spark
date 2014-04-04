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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.io.Resources;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.utils.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.stratio.deep.utils.Utils.quote;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Base class for all Deep Examples integration tests.
 */
public class AbstractDeepExamplesTest {
    private Logger logger = Logger.getLogger(getClass());
    protected static DeepSparkContext context;
    private static CassandraServer cassandraServer;
    protected static Session session;

    public static final String KEYSPACE_NAME = "tutorials";
    public static final String TWEETS_COLUMN_FAMILY = "tweets";
    public static final String CRAWLER_COLUMN_FAMILY = "Page";
    public static final String OUTPUT_KEYSPACE_NAME = "out_tutorials";
    public static final String OUTPUT_TWEETS_COLUMN_FAMILY = "out_tweets";

    private static final String createTweetCF = String.format(
            "CREATE TABLE %s (" +
            "  tweet_id uuid PRIMARY KEY," +
            "  tweet_date timestamp," +
            "  author text," +
            "  hashtags text," +
            "  favorite_count int," +
            "  content text," +
            "  truncated boolean" +
            ");", TWEETS_COLUMN_FAMILY);

    private static final String createCrawlerCF = String.format(
            "CREATE TABLE \"%s\" (\n"+
            " key text,\n"+
            " \"___class\" text,\n"+
            " charset text,\n"+
            " content text,\n"+
            " \"domainName\" text,\n"+
            " \"downloadTime\" bigint,\n"+
            " \"enqueuedForTransforming\" bigint,\n"+
            " etag text,\n"+
            " \"firstDownloadTime\" bigint,\n"+
            " \"lastModified\" text,\n"+
            " \"responseCode\" varint,\n"+
            " \"responseTime\" bigint,\n"+
            " \"timeTransformed\" bigint,\n"+
            " title text,\n"+
            " url text,\n"+
            " PRIMARY KEY (key)\n"+
            ");", CRAWLER_COLUMN_FAMILY);

    @BeforeSuite
    protected void initContextAndServer() throws ConfigurationException, IOException, InterruptedException {
        logger.info("instantiating context");
        //context = new DeepSparkContext("local", "deepSparkContextTest");

        String createKeyspace = "CREATE KEYSPACE " + KEYSPACE_NAME
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";

        String createOutputKeyspace = "CREATE KEYSPACE " + OUTPUT_KEYSPACE_NAME
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";

        String useKeyspace = "USE " + KEYSPACE_NAME + ";";

        String useOutputKeyspace = "USE " + OUTPUT_KEYSPACE_NAME + ";";


        String[] startupCommands =
                new String[] {createKeyspace, createOutputKeyspace, useKeyspace, createTweetCF, createCrawlerCF, useOutputKeyspace};

        cassandraServer = new CassandraServer();
        cassandraServer.setStartupCommands(startupCommands);
        cassandraServer.start();

        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();

        session = cluster.connect();

        dataInsertCql();

        checkTestData();
    }

    @AfterSuite
    protected void disposeServerAndRdd() throws IOException {
        if (session != null){
            session.close();
        }

        if (cassandraServer != null) {
            cassandraServer.shutdown();
        }

        if (context != null) {
            context.stop();
        }
    }

    protected void dataInsertCql() {
        URL tweetsTestData = Resources.getResource("tutorials-tweets.csv");
        URL crawlerTestData = Resources.getResource("crawler-Page-extract.csv.zip");

        List<Insert> inserts = new ArrayList<>();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] names = new String[]{"tweet_id", "tweet_date", "author", "hashtags", "favorite_count", "content", "truncated"};
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
                new File(tweetsTestData.toURI()))))) {

            String line;

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");

                Object[] values = new Object[fields.length];

                values[0] = UUID.fromString(fields[0]);
                values[1] = sdf.parse(fields[1]);
                values[2] = fields[2];
                values[3] = fields[3];
                values[4] = Integer.parseInt(fields[4]);
                values[5] = fields[5];
                values[6] = Boolean.parseBoolean(fields[6]);

                Insert insert = QueryBuilder.insertInto(KEYSPACE_NAME,TWEETS_COLUMN_FAMILY)
                        .values(names, values);

                inserts.add(insert);
            }

            Batch batch = QueryBuilder.batch(inserts.toArray(new Insert[0]));

            session.execute(batch);
        } catch (Exception e) {
            logger.error("Error", e);
        }

        names = new String[]{
                "key", "\"___class\"", "charset", "content", "\"domainName\"",
                "\"downloadTime\"", "\"enqueuedForTransforming\"", "etag", "\"firstDownloadTime\"",
                "\"lastModified\"", "\"responseCode\"", "\"responseTime\"", "\"timeTransformed\"", "title", "url"};

        try (ZipInputStream zis = new ZipInputStream( new FileInputStream(
                new File(crawlerTestData.toURI())))){

            ZipEntry entry = zis.getNextEntry();

        } catch (Exception e) {
            logger.error("Error", e);
        }

        /*
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new ZipInputStream( new FileInputStream(
                new File(crawlerTestData.toURI())))))) {

            String line;

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");

                Object[] values = new Object[fields.length];

                values[0] = fields[0];
                values[1] = fields[1];
                values[2] = fields[2];
                values[3] = fields[3];
                values[4] = fields[4];
                values[5] = Long.parseLong(fields[5]);
                values[6] = Long.parseLong(fields[6]);
                values[7] = fields[7];
                values[8] = Long.parseLong(fields[8]);
                values[9] = fields[9];

                values[10] = new BigInteger(fields[10]);
                values[11] = Long.parseLong(fields[11]);
                values[12] = Long.parseLong(fields[12]);
                values[13] = fields[13];
                values[14] = fields[14];

                Insert insert = QueryBuilder.insertInto(KEYSPACE_NAME,CRAWLER_COLUMN_FAMILY)
                        .values(names, values);

                inserts.add(insert);
            }

            Batch batch = QueryBuilder.batch(inserts.toArray(new Insert[0]));

            session.execute(batch);
        } catch (Exception e) {
            logger.error("Error", e);
        }
        */
    }

    private void checkTestData() {
        String command = "select count(*) from " + KEYSPACE_NAME + "." + quote(TWEETS_COLUMN_FAMILY) + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 999);


        command = "select count(*) from " + KEYSPACE_NAME + "." + quote(CRAWLER_COLUMN_FAMILY) + ";";

        rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 100);
    }
}
