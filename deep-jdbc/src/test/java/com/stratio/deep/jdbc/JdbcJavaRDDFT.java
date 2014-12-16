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

package com.stratio.deep.jdbc;

import org.apache.derby.jdbc.EmbeddedDriver;
import org.hsqldb.jdbc.JDBCDriver;
import org.hsqldb.jdbcDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;

import static org.testng.Assert.assertEquals;

/**
 * Generic Functional Test for JDBC Extractor
 */
@Test(groups = { "JdbcJavaRDDFT", "FunctionalTests" })
public class JdbcJavaRDDFT {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcJavaRDDFT.class);

    private Connection connCell;

    private Connection connEntity;

    public static final String HOST = null;

    public static final int PORT = -1;

    public static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";

    public static final String USER = "SA";

    public static final String PASSWORD = "";

    public static final String NAMESPACE_CELL = "jdbccellextractor";

    public static final String NAMESPACE_ENTITY = "jdbcentityextractor";

    public static final String INPUT_TABLE = "input";

    public static final String OUTPUT_CELLS_TABLE = "outputCells";

    public static final String OUTPUT_ENTITY_TABLE = "outputEntity";

    public static final String SET_NAME_BOOK = "bookinput";

    @BeforeSuite
    public void init() throws Exception {
        DriverManager.registerDriver(new JDBCDriver());
        connCell = DriverManager.getConnection("jdbc:hsqldb:file:" + NAMESPACE_CELL);
        createTables(connCell, NAMESPACE_CELL, INPUT_TABLE, OUTPUT_CELLS_TABLE);
        deleteData(connCell, INPUT_TABLE, OUTPUT_CELLS_TABLE);
        connEntity = DriverManager.getConnection("jdbc:hsqldb:file:" + NAMESPACE_ENTITY);
        createTables(connEntity, NAMESPACE_ENTITY, INPUT_TABLE, OUTPUT_ENTITY_TABLE);
        deleteData(connEntity, INPUT_TABLE, OUTPUT_ENTITY_TABLE);

    }

    @Test
    public void testRDD() {
        assertEquals(true, true, "Dummy test");
    }

    private void createTables(Connection conn, String namespace, String inputTable, String outputTable) throws Exception {
        Statement stmnt = conn.createStatement();
        try {
            stmnt.executeUpdate("CREATE TABLE \"" + inputTable + "\"(\"id\" VARCHAR(255) NOT NULL, \"message\" VARCHAR(255), \"number\" BIGINT, PRIMARY KEY (\"id\"))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + inputTable + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE TABLE \"" + outputTable + "\"(\"id\" VARCHAR(255) NOT NULL, \"message\" VARCHAR(255), \"number\" BIGINT, PRIMARY KEY (\"id\"))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + outputTable + " (maybe is it already created?)", e);
        }
    }

    private void deleteData(Connection conn, String inputTable, String outputTable) throws Exception {
        Statement statement = conn.createStatement();
        try {
            statement.executeUpdate("DELETE FROM \"" + inputTable + "\"");
        } catch (SQLException e) {
            LOG.error("Error deleting table " + inputTable + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM \"" + outputTable + "\"");
        } catch (SQLException e) {
            LOG.error("Error deleting table " + outputTable + ", does the table exist?");
        }
    }

    @AfterSuite
    public void cleanup() throws Exception {
        connCell.close();
        connEntity.close();
    }
}
