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

package com.stratio.deep.cassandra.extractor;

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
import org.testng.annotations.Test;

import static com.stratio.deep.commons.utils.Utils.quote;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


@Test(suiteName = "cassandraddTests", groups = { "cassandraJavaRDDTest" })
public class CassandraJavaRDDTest {

    private Logger logger = Logger.getLogger(getClass());

    private static CassandraServer cassandraServer;
    public static final String KEYSPACE_NAME = "Test_Keyspace";
    public static final String COLUMN_FAMILY = "test_Page";
    public static final String OUTPUT_KEYSPACE_NAME = "out_test_keyspace";
    public static final String CQL3_COLUMN_FAMILY = "cql3_cf";
    public static final String CQL3_COLLECTION_COLUMN_FAMILY = "cql3_collection_cf";



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



    @AfterSuite
    protected void disposeServerAndRdd() throws IOException {
        if (cassandraServer != null) {
            cassandraServer.shutdown();
        }

    }


    @Test
    public void test() {
        assertEquals(true, true);
    }

    @BeforeSuite
    protected void initContextAndServer() throws ConfigurationException, IOException, InterruptedException {
        logger.info("instantiating context");

        String createKeyspace = "CREATE KEYSPACE " + quote(KEYSPACE_NAME)
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";

        String createOutputKeyspace = "CREATE KEYSPACE " + OUTPUT_KEYSPACE_NAME
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";



        String[] startupCommands = new String[]{createKeyspace, createOutputKeyspace,  createCF,
                createCFIndex, /*createLuceneIndex,*/
                createCql3CF, createCql3CFIndex, createCql3CollectionsCF, };

        cassandraServer = new CassandraServer();
        cassandraServer.setStartupCommands(startupCommands);
        cassandraServer.start();

    }


}
