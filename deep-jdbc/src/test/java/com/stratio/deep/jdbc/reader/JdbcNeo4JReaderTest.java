package com.stratio.deep.jdbc.reader;

import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import org.apache.spark.rdd.JdbcPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ JdbcNeo4JReader.class })
public class JdbcNeo4JReaderTest {

    private static final String COLUMN_NAME1_CONSTANT = "column_name_1";

    private static final String COLUMN_NAME2_CONSTANT = "column_name_2";

    private static final String COLUMN_VALUE1_CONSTANT = "column_value_1";

    private static final String COLUMN_VALUE2_CONSTANT = "column_value_2";

    private static final String JDBC_CELL_EXTRACTOR_CLASSNAME_CONSTANT = "com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor";

    private static final String WHATEVER_CONSTANT = "whatever";

    private static final int NUM_PARTITIONS = 2;

    @Mock
    private JdbcNeo4JDeepJobConfig<?> config;

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
        JdbcNeo4JReader reader = new JdbcNeo4JReader(config);

        Class<?> clazz = JdbcNeo4JReader.class;
        Field resultSetField = clazz.getSuperclass().getDeclaredField("resultSet");
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
        JdbcNeo4JReader reader = new JdbcNeo4JReader(config);

        Class<?> clazz = JdbcNeo4JReader.class;
        Field hasNextField = clazz.getSuperclass().getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        Field resultSetField = clazz.getSuperclass().getDeclaredField("resultSet");
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
        JdbcNeo4JReader reader = new JdbcNeo4JReader(config);

        Class<?> clazz = JdbcNeo4JReader.class;
        Field hasNextField = clazz.getSuperclass().getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        Field resultSetField = clazz.getSuperclass().getDeclaredField("resultSet");
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
        JdbcNeo4JReader reader = new JdbcNeo4JReader(config);

        Class<?> clazz = JdbcNeo4JReader.class;
        Field hasNextField = clazz.getSuperclass().getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        Field resultSetField = clazz.getSuperclass().getDeclaredField("resultSet");
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

        when(config.getConnectionUrl()).thenReturn(WHATEVER_CONSTANT);
        when(config.getUsername()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPassword()).thenReturn(WHATEVER_CONSTANT);
        when(config.getQuery()).thenReturn(mock(SelectQuery.class));
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        // Setting up test scenario
        JdbcNeo4JReader reader = new JdbcNeo4JReader(config);

        reader.init(partition);

        // Assertions
        Class<?> clazz = JdbcNeo4JReader.class;
        Field hasNextField = clazz.getSuperclass().getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        assertEquals(hasNextField.get(reader), false, "hasNext field should be set to FALSE");
    }

    @Test
    public void testNextRowOnInitWhenResultSetIsNotEmpty() throws Exception {

        // Stubbing external objects
        PowerMockito.mockStatic(DriverManager.class);

        when(config.getConnectionUrl()).thenReturn(WHATEVER_CONSTANT);
        when(config.getUsername()).thenReturn(WHATEVER_CONSTANT);
        when(config.getPassword()).thenReturn(WHATEVER_CONSTANT);
        when(config.getQuery()).thenReturn(mock(SelectQuery.class));
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        // Setting up test scenario
        JdbcNeo4JReader reader = new JdbcNeo4JReader(config);

        reader.init(partition);

        // Assertions
        Class<?> clazz = JdbcNeo4JReader.class;
        Field hasNextField = clazz.getSuperclass().getDeclaredField("hasNext");
        hasNextField.setAccessible(true);
        assertEquals(hasNextField.get(reader), true, "hasNext field should be set to TRUE");
    }

}
