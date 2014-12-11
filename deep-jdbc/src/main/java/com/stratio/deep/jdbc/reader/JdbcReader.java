package com.stratio.deep.jdbc.reader;

import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcReader {

    private JdbcDeepJobConfig jdbcDeepJobConfig;

    private Connection conn;

    private ResultSet resultSet;

    public JdbcReader(JdbcDeepJobConfig config) {
        this.jdbcDeepJobConfig = config;
    }

    public void init(Partition p) throws Exception {
        JdbcPartition partition = (JdbcPartition)p;
        Class.forName(jdbcDeepJobConfig.getDriverClass());
        conn = DriverManager.getConnection(jdbcDeepJobConfig.getJdbcUrl(),
                jdbcDeepJobConfig.getUsername(),
                jdbcDeepJobConfig.getPassword());
        Statement statement = conn.createStatement();
        resultSet = statement.executeQuery(jdbcDeepJobConfig.getQuery());
    }

    public boolean hasNext() throws SQLException {
        return resultSet.next();
    }

    public Map<String, Object> next() throws SQLException {
        Map<String, Object> row = new HashMap<>();
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnsNumber = metadata.getColumnCount();
        for(int i=1; i<=columnsNumber; i++) {
            String columnName = metadata.getColumnName(i);
            row.put(columnName, resultSet.getObject(i));
        }
        return row;
    }

    public void close() throws SQLException {
        try {
            if(resultSet != null) {
                resultSet.close();
            }
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}
