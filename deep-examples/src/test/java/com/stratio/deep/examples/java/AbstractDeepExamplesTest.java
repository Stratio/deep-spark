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

import au.com.bytecode.opencsv.CSVReader;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.io.Resources;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.utils.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static com.stratio.deep.utils.Utils.quote;
import static org.testng.Assert.assertEquals;

/**
 * Base class for all Deep Examples integration tests.
 */
public class AbstractDeepExamplesTest {
    private Logger logger = Logger.getLogger(getClass());
    protected static DeepSparkContext context;
    private static CassandraServer cassandraServer;
    protected static Session session;

    public static final String KEYSPACE_NAME = "test";
    public static final String CRAWLER_KEYSPACE_NAME = "crawler";
    public static final String TWEETS_COLUMN_FAMILY = "tweets";
    public static final String CRAWLER_COLUMN_FAMILY = "Page";
    public static final String OUTPUT_KEYSPACE_NAME = "out_test";
    public static final String OUTPUT_TWEETS_COLUMN_FAMILY = "out_tweets";

    private static final String createTweetCF = String.format(
            "CREATE TABLE %s (" +
                    "  tweet_id uuid PRIMARY KEY," +
                    "  tweet_date timestamp," +
                    "  author text," +
                    "  hashtags set<text>," +
                    "  favorite_count int," +
                    "  content text," +
                    "  truncated boolean" +
                    ");", TWEETS_COLUMN_FAMILY
    );

    private static final String createCrawlerCF = String.format(
            "CREATE TABLE %s.\"%s\" (\n" +
                    " key text,\n" +
                    " \"___class\" text,\n" +
                    " charset text,\n" +
                    " content text,\n" +
                    " \"domainName\" text,\n" +
                    " \"downloadTime\" bigint,\n" +
                    " \"enqueuedForTransforming\" bigint,\n" +
                    " etag text,\n" +
                    " \"firstDownloadTime\" bigint,\n" +
                    " \"lastModified\" text,\n" +
                    " \"responseCode\" varint,\n" +
                    " \"responseTime\" bigint,\n" +
                    " \"timeTransformed\" bigint,\n" +
                    " title text,\n" +
                    " url text,\n" +
                    " PRIMARY KEY (key)\n" +
                    ");", CRAWLER_KEYSPACE_NAME, CRAWLER_COLUMN_FAMILY
    );

    @BeforeSuite
    protected void initContextAndServer() throws ConfigurationException, IOException, InterruptedException {
        logger.info("instantiating context");

        context = new DeepSparkContext("local", "deepSparkContextTest");

        String createKeyspace = "CREATE KEYSPACE " + KEYSPACE_NAME
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2 };";

        String createCrawlerKeyspace = "CREATE KEYSPACE " + CRAWLER_KEYSPACE_NAME
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2 };";

        String useKeyspace = "USE " + KEYSPACE_NAME + ";";

        String[] startupCommands =
                new String[]{createKeyspace, createCrawlerKeyspace, useKeyspace, createTweetCF, createCrawlerCF};

        cassandraServer = new CassandraServer();
        cassandraServer.setStartupCommands(startupCommands);
        cassandraServer.start();
//
//        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
//                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
//
//        session = cluster.connect();
//
//
//        try {
//
//            session.execute(createKeyspace);
//            session.execute(createCrawlerKeyspace);
//            session.execute(useKeyspace);
//            session.execute(createTweetCF);
//            session.execute(createCrawlerCF);
//        } catch (Exception ex){}
//
//        dataInsertCql();
//
//        checkTestData();
    }

    @AfterSuite
    protected void disposeServerAndRdd() throws IOException {
        if (session != null) {
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
        URL tweetsTestData = Resources.getResource("test-tweets.csv");
        URL crawlerTestData = Resources.getResource("crawler-Page-extract.csv.gz");

        List<Insert> inserts = new ArrayList<>();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] names = new String[]{"tweet_id", "tweet_date", "author", "hashtags", "favorite_count", "content",
                "truncated"};
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
                new File(tweetsTestData.toURI()))))) {

            String line;

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(";");

                Object[] values = new Object[fields.length];

                values[0] = UUID.fromString(fields[0]);
                values[1] = sdf.parse(fields[1]);
                values[2] = fields[2];
                if (fields[3].length() > 2) {
                    String[] hashtags = fields[3].substring(1, fields[3].length() - 2).split(",");
                    values[3] = new HashSet<String>(Arrays.asList(hashtags));
                } else {
                    values[3] = new HashSet<String>();
                }
                values[4] = Integer.parseInt(fields[4]);
                values[5] = fields[5];
                values[6] = Boolean.parseBoolean(fields[6]);

                Insert insert = QueryBuilder.insertInto(KEYSPACE_NAME, TWEETS_COLUMN_FAMILY)
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

        try (CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(new GZIPInputStream(new
                FileInputStream(
                new File(crawlerTestData.toURI()))))))) {

            String[] fields;

            while ((fields = reader.readNext()) != null) {

                Object[] values = new Object[fields.length];

                try {
                    values[0] = ne(fields[0]);
                    values[1] = ne(fields[1]);
                    values[2] = ne(fields[2]);
                    values[3] = ne(fields[3]);
                    values[4] = ne(fields[4]);
                    values[5] = ne(fields[5]) == null ? null : Long.parseLong(ne(fields[5]));
                    values[6] = ne(fields[6]) == null ? null : Long.parseLong(ne(fields[6]));
                    values[7] = ne(fields[7]);
                    values[8] = ne(fields[8]) == null ? null : Long.parseLong(ne(fields[8]));
                    values[9] = ne(fields[9]);

                    values[10] = ne(fields[10]) == null ? null : new BigInteger(ne(fields[10]));
                    values[11] = ne(fields[11]) == null ? null : Long.parseLong(ne(fields[11]));
                    values[12] = ne(fields[12]) == null ? null : Long.parseLong(ne(fields[12]));
                    values[13] = ne(fields[13]);
                    values[14] = ne(fields[14]);

                    Insert insert = QueryBuilder.insertInto(CRAWLER_KEYSPACE_NAME, quote(CRAWLER_COLUMN_FAMILY))
                            .values(names, values);

                    inserts.add(insert);
                } catch (NumberFormatException e) {
                    logger.error("Cannot parse line: " + fields);
                }
            }


            Batch batch = QueryBuilder.batch(inserts.toArray(new Insert[0]));
            session.execute(batch);
        } catch (Exception e) {
            logger.error("Error", e);
        }

    }

    private String ne(String val) {
        if (StringUtils.isEmpty(val)) {
            return null;
        }
        return val.trim();
    }

    private void checkTestData() {
        String command = "select count(*) from " + KEYSPACE_NAME + "." + quote(TWEETS_COLUMN_FAMILY) + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 4892);


        command = "select count(*) from " + CRAWLER_KEYSPACE_NAME + "." + quote(CRAWLER_COLUMN_FAMILY) + ";";

        rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 98);
    }
}
