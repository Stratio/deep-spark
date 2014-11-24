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

package com.stratio.deep.cassandra;

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
import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.cassandra.extractor.CassandraEntityExtractor;
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


@Test(suiteName = "cassandraExtractorTests", groups = { "cassandraJavaRDDFT" })
public class CassandraJavaRDDFT {

    private Logger logger = Logger.getLogger(getClass());

    private static CassandraServer cassandraServer;
    public static final String KEYSPACE_NAME_1 = CassandraCellExtractor.class.getSimpleName().toLowerCase();
    public static final String KEYSPACE_NAME_2 = CassandraEntityExtractor.class.getSimpleName().toLowerCase();



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



        String createKeyspace1 = "CREATE KEYSPACE " + quote(KEYSPACE_NAME_1)
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";

        String createKeyspace2 = "CREATE KEYSPACE " + KEYSPACE_NAME_2
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1 };";



        String[] startupCommands = new String[]{createKeyspace1, createKeyspace2};

        cassandraServer = new CassandraServer();
        cassandraServer.setStartupCommands(startupCommands);
        cassandraServer.start();

    }


}
