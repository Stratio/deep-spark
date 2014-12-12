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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;

/**
 * Created by mariomgal on 11/12/14.
 */
@Test(groups = { "JdbcJavaRDDFT", "FunctionalTests" })
public class JdbcJavaRDDFT {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcJavaRDDFT.class);

    private Connection connCell;

    private Connection connEntity;

    private Statement statement;

    public static final String HOST = "127.0.0.1";

    public static final int PORT = 3306;

    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    public static final String USER = "root";

    public static final String PASSWORD = "root";

    public static final String NAMESPACE_CELL = "jdbccellextractor";

    public static final String NAMESPACE_ENTITY = "jdbcentityextractor";

    public static final String INPUT_TABLE = "input";

    public static final String OUTPUT_CELLS_TABLE = "outputcells";

    public static final String OUTPUT_ENTITY_TABLE = "outputentity";

    public static final String SET_NAME_BOOK = "bookinput";

    @BeforeSuite
    public void init() throws Exception {
        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        createDatabases();
        connCell = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/" + NAMESPACE_CELL, USER, PASSWORD);
        statement = connCell.createStatement();
        deleteData();
        connEntity = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/" + NAMESPACE_ENTITY, USER, PASSWORD);
        statement = connEntity.createStatement();
        deleteData();
    }

    @Test
    public void testRDD() {
        assertEquals(true, true, "Dummy test");
    }

    private void createDatabases() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/", USER, PASSWORD);
        Statement stmnt = conn.createStatement();
        try {
            stmnt.executeUpdate("CREATE DATABASE " + NAMESPACE_CELL);
        } catch(SQLException e) {
            LOG.error("Problem while creating database " + NAMESPACE_CELL + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE TABLE " + NAMESPACE_CELL + "." + INPUT_TABLE + "(id VARCHAR(255) NOT NULL, message VARCHAR(255), number INTEGER, PRIMARY KEY (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + INPUT_TABLE + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE TABLE " + NAMESPACE_CELL + "." + OUTPUT_CELLS_TABLE + "(id VARCHAR(255) NOT NULL, message VARCHAR(255), number INTEGER, PRIMARY KEY (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + OUTPUT_CELLS_TABLE + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE DATABASE " + NAMESPACE_ENTITY);
        } catch(SQLException e) {
            LOG.error("Problem while creating database " + NAMESPACE_ENTITY + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE TABLE " + NAMESPACE_ENTITY + "." + INPUT_TABLE + "(id VARCHAR(255) NOT NULL, message VARCHAR(255), number BIGINT, PRIMARY KEY (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + INPUT_TABLE + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE TABLE " + NAMESPACE_ENTITY + "." + OUTPUT_ENTITY_TABLE + "(id VARCHAR(255) NOT NULL, message VARCHAR(255), number BIGINT, PRIMARY KEY (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + OUTPUT_ENTITY_TABLE + " (maybe is it already created?)", e);
        }
        conn.close();
    }

    private void deleteData() throws Exception {
        try {
            statement.executeUpdate("DELETE FROM " + INPUT_TABLE);
        } catch(SQLException e) {
            LOG.error("Error deleting table " + INPUT_TABLE + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM " + OUTPUT_CELLS_TABLE);
        } catch(SQLException e) {
            LOG.error("Error deleting table " + OUTPUT_CELLS_TABLE + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM " + OUTPUT_ENTITY_TABLE);
        } catch(SQLException e) {
            LOG.error("Error deleting table " + OUTPUT_ENTITY_TABLE + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM " + SET_NAME_BOOK);
        } catch(SQLException e) {
            LOG.error("Error deleting table " + SET_NAME_BOOK + ", does the table exist?");
        }
    }

    @AfterSuite
    public void cleanup() throws Exception {
        connCell.close();
        connEntity.close();
    }
}
