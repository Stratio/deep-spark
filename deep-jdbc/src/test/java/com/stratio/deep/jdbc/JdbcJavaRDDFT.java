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

import org.h2.Driver;
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

    public static final Class DRIVER_CLASS = Driver.class;

    public static final String USER = "SA";

    public static final String PASSWORD = "";

    public static final String NAMESPACE_CELL = "jdbccellextractor";

    public static final String NAMESPACE_ENTITY = "jdbcentityextractor";

    public static final String INPUT_TABLE = "input";

    public static final String OUTPUT_CELLS_TABLE = "outputCells";

    public static final String OUTPUT_ENTITY_TABLE = "outputEntity";

    @BeforeSuite
    public void init() throws Exception {
        DriverManager.registerDriver(new Driver());
        connCell = DriverManager.getConnection("jdbc:h2:~/" + NAMESPACE_CELL + ";DATABASE_TO_UPPER=FALSE", "sa", "");
        createTables(connCell, NAMESPACE_CELL, INPUT_TABLE, OUTPUT_CELLS_TABLE);
        deleteData(connCell, NAMESPACE_CELL, INPUT_TABLE, OUTPUT_CELLS_TABLE);
        connEntity = DriverManager.getConnection("jdbc:h2:~/" + NAMESPACE_ENTITY + ";DATABASE_TO_UPPER=FALSE", "sa", "");
        createTables(connEntity, NAMESPACE_ENTITY, INPUT_TABLE, OUTPUT_ENTITY_TABLE);
        deleteData(connEntity, NAMESPACE_ENTITY, INPUT_TABLE, OUTPUT_ENTITY_TABLE);

    }

    @Test
    public void testRDD() {
        assertEquals(true, true, "Dummy test");
    }

    private void createTables(Connection conn, String namespace, String inputTable, String outputTable) throws Exception {
        Statement stmnt = conn.createStatement();
        stmnt.executeUpdate("CREATE SCHEMA IF NOT EXISTS " + namespace + " AUTHORIZATION " + USER);
        try {
            stmnt.executeUpdate("CREATE TABLE " + namespace + "." + inputTable + "(id VARCHAR(255) NOT NULL, message VARCHAR(255), number BIGINT, PRIMARY KEY (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + namespace + "." + inputTable + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("CREATE TABLE " + namespace + "." + outputTable + "(id VARCHAR(255) NOT NULL, message VARCHAR(255), number BIGINT, PRIMARY KEY (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + namespace + "." + outputTable + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("create table " + namespace + "." + "footballteam (id bigint not null, name varchar(255) not null, "
                    + "short_name varchar(255) not null, "
                    + "arena_name varchar(255) not null, coach_name varchar(255) not null, city_name varchar(255) not null, league_name varchar(255) not null, "
                    + "primary key (id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + "footballteam" + " (maybe is it already created?)", e);
        }
        try {
            stmnt.executeUpdate("create table " + namespace + "." + "footballplayer (id bigint not null, firstname varchar(255) not "
                    + "null, lastname varchar(255) not null, "
                    + "date_of_birth varchar(255) not null, place_of_birth varchar(255) not null, position_name varchar(255) not null, team_id bigint not null, "
                    + "primary key (id), foreign key (team_id) references footballteam(id))");
        } catch(SQLException e) {
            LOG.error("Problem while creating table " + "footballplayer" + " (maybe is it already created?)", e);
        }
    }

    private void deleteData(Connection conn, String namespace, String inputTable, String outputTable) throws Exception {
        Statement statement = conn.createStatement();
        try {
            statement.executeUpdate("DELETE FROM " + namespace + "." + inputTable + "");
        } catch (SQLException e) {
            LOG.error("Error deleting table " + inputTable + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM " + namespace + "." + outputTable + "");
        } catch (SQLException e) {
            LOG.error("Error deleting table " + outputTable + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM " + namespace + ".footballplayer" + "");
        } catch (SQLException e) {
            LOG.error("Error deleting table " + "footballplayer" + ", does the table exist?");
        }
        try {
            statement.executeUpdate("DELETE FROM " + namespace + ".footballteam" + "");
        } catch (SQLException e) {
            LOG.error("Error deleting table " + "footballteam" + ", does the table exist?");
        }

    }

    @AfterSuite
    public void cleanup() throws Exception {
        connCell.close();
        connEntity.close();
    }
}
