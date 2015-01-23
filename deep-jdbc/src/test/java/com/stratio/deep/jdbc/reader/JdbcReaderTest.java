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

package com.stratio.deep.jdbc.reader;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.ComboCondition;
import com.healthmarketscience.sqlbuilder.Condition;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ JdbcReader.class })
public class JdbcReaderTest {

    private static final String COLUMN_NAME1_CONSTANT = "column_name_1";

    private static final String COLUMN_NAME2_CONSTANT = "column_name_2";

    private static final String COLUMN_VALUE1_CONSTANT = "column_value_1";

    private static final String COLUMN_VALUE2_CONSTANT = "column_value_2";

    private static final String JDBC_CELL_EXTRACTOR_CLASSNAME_CONSTANT = "com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor";

    private static final String WHATEVER_CONSTANT = "whatever";

    private static final int NUM_PARTITIONS = 2;


    @Mock
    private JdbcDeepJobConfig<?> config;

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData metadata;

    @Mock
    private JdbcPartition partition;

    @Mock
    private Connection conn;

    @Mock
    private Statement statement;

    @Test
    public void testRowMappingWhenCallingNext() throws SQLException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException {

        // Stubbing external objects
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnName(1)).thenReturn(COLUMN_NAME1_CONSTANT);
        when(metadata.getColumnName(2)).thenReturn(COLUMN_NAME2_CONSTANT);
        when(resultSet.getObject(1)).thenReturn(COLUMN_VALUE1_CONSTANT);
        when(resultSet.getObject(2)).thenReturn(COLUMN_VALUE2_CONSTANT);

        // Setting up test scenario
        JdbcReader reader = new JdbcReader(config);

        Class<?> clazz = JdbcReader.class;
        Field resultSetField = clazz.getDeclaredField("resultSet");
        resultSetField.setAccessible(true);

        resultSetField.set(reader, resultSet);

        Map<String, Object> rowMap = reader.next();

        // Assertions
        assertEquals((String) rowMap.get(COLUMN_NAME1_CONSTANT), COLUMN_VALUE1_CONSTANT, "Column value not expected");
        assertEquals((String) rowMap.get(COLUMN_NAME2_CONSTANT), COLUMN_VALUE2_CONSTANT, "Column value not expected");

        verify(resultSet, times(1)).next();
    }

    @Test
    public void testFirstNextWithSecondNext() throws NoSuchFieldException, SecurityException, SQLException,
            IllegalArgumentException, IllegalAccessException {

        // Stubbing external objects
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(0);

        // Setting up test scenario
        JdbcReader reader = new JdbcReader(config);

        Class<?> clazz = JdbcReader.class;
        Field hasNextField = clazz.getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        Field resultSetField = clazz.getDeclaredField("resultSet");
        resultSetField.setAccessible(true);

        hasNextField.set(reader, true);
        resultSetField.set(reader, resultSet);

        reader.next();

        // Assertions
        assertEquals(hasNextField.get(reader), true, "hasNext field should be set to TRUE");

        verify(resultSet, times(1)).next();
    }

    @Test
    public void testFirstNextWithNoSecondNext() throws SQLException, NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {

        // Stubbing external objects
        when(resultSet.next()).thenReturn(false);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(0);

        // Setting up test scenario
        JdbcReader reader = new JdbcReader(config);

        Class<?> clazz = JdbcReader.class;
        Field hasNextField = clazz.getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        Field resultSetField = clazz.getDeclaredField("resultSet");
        resultSetField.setAccessible(true);

        hasNextField.set(reader, true);
        resultSetField.set(reader, resultSet);

        reader.next();

        // Assertions
        assertEquals(hasNextField.get(reader), false, "hasNext field should be set to FALSE");

        verify(resultSet, times(1)).next();
    }

    @Test(expected = SQLException.class)
    public void testNextWithoutNext() throws SQLException, NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {

        // Stubbing external objects
        when(resultSet.getMetaData()).thenThrow(SQLException.class);

        // Setting up test scenario
        JdbcReader reader = new JdbcReader(config);

        Class<?> clazz = JdbcReader.class;
        Field hasNextField = clazz.getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        Field resultSetField = clazz.getDeclaredField("resultSet");
        resultSetField.setAccessible(true);

        hasNextField.set(reader, false);
        resultSetField.set(reader, resultSet);

        reader.next();

        // Assertions
        assertEquals(hasNextField.get(reader), false, "hasNext field should be set to FALSE");

        verify(resultSet, times(1)).next();
    }

    @Test
    public void testNextRowOnInitWhenResultSetIsEmpty() throws Exception {

        // Stubbing external objects
        PowerMockito.mockStatic(DriverManager.class);

        when(config.getDriverClass()).thenReturn(JDBC_CELL_EXTRACTOR_CLASSNAME_CONSTANT);
        when(config.getConnectionUrl()).thenReturn(WHATEVER_CONSTANT);
        when(config.getUsername()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPassword()).thenReturn(WHATEVER_CONSTANT);
        when(config.getQuery()).thenReturn(mock(SelectQuery.class));
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        // Setting up test scenario
        JdbcReader reader = new JdbcReader(config);

        reader.init(partition);

        // Assertions
        Class<?> clazz = JdbcReader.class;
        Field hasNextField = clazz.getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        assertEquals(hasNextField.get(reader), false, "hasNext field should be set to FALSE");
    }

    @Test
    public void testNextRowOnInitWhenResultSetIsNotEmpty() throws Exception {

        // Stubbing external objects
        PowerMockito.mockStatic(DriverManager.class);

        when(config.getDriverClass()).thenReturn(JDBC_CELL_EXTRACTOR_CLASSNAME_CONSTANT);
        when(config.getConnectionUrl()).thenReturn(WHATEVER_CONSTANT);
        when(config.getUsername()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPassword()).thenReturn(WHATEVER_CONSTANT);
        when(config.getQuery()).thenReturn(mock(SelectQuery.class));
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        // Setting up test scenario
        JdbcReader reader = new JdbcReader(config);

        reader.init(partition);

        // Assertions
        Class<?> clazz = JdbcReader.class;
        Field hasNextField = clazz.getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        assertEquals(hasNextField.get(reader), true, "hasNext field should be set to TRUE");
    }

    @Test
    public void testConditionIsAddedForPartitioning() throws Exception {

        PowerMockito.mockStatic(DriverManager.class);
        SelectQuery selectQuery = PowerMockito.mock(SelectQuery.class);
        ComboCondition comboCondition = PowerMockito.mock(ComboCondition.class);

        when(selectQuery.getWhereClause()).thenReturn(comboCondition);
        when(comboCondition.addCondition(any(Condition.class))).thenReturn(comboCondition);
        when(config.getDriverClass()).thenReturn(JDBC_CELL_EXTRACTOR_CLASSNAME_CONSTANT);
        when(config.getConnectionUrl()).thenReturn(WHATEVER_CONSTANT);
        when(config.getUsername()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPassword()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPartitionKey()).thenReturn(PowerMockito.mock(DbColumn.class));
        when(config.getNumPartitions()).thenReturn(NUM_PARTITIONS);
        when(config.getQuery()).thenReturn(selectQuery);
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
        when(partition.lower()).thenReturn(0L);
        when(partition.upper()).thenReturn(100L);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        JdbcReader reader = new JdbcReader(config);

        reader.init(partition);

        verify(comboCondition, times(2)).addCondition(any((BinaryCondition.class)));
    }

    @Test
    public void testNoConditionsAddedIfNotPartitioning() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);
        SelectQuery selectQuery = PowerMockito.mock(SelectQuery.class);
        ComboCondition comboCondition = PowerMockito.mock(ComboCondition.class);

        when(selectQuery.getWhereClause()).thenReturn(comboCondition);
        when(comboCondition.addCondition(any(Condition.class))).thenReturn(comboCondition);
        when(config.getDriverClass()).thenReturn(JDBC_CELL_EXTRACTOR_CLASSNAME_CONSTANT);
        when(config.getConnectionUrl()).thenReturn(WHATEVER_CONSTANT);
        when(config.getUsername()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPassword()).thenReturn(WHATEVER_CONSTANT);
        when(config.getQuery()).thenReturn(selectQuery);
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        JdbcReader reader = new JdbcReader(config);

        reader.init(partition);

        verify(comboCondition, times(0)).addCondition(any((BinaryCondition.class)));

    }

}