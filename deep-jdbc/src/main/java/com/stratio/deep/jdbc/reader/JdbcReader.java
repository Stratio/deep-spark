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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.Column;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;

/**
 * Creates a new JDBC connection and provides methods for reading from it.
 */
public class JdbcReader implements AutoCloseable {

    /**
     * JDBC Deep Job configuration.
     */
    private final JdbcDeepJobConfig jdbcDeepJobConfig;

    /**
     * JDBC Connection.
     */
    private Connection conn;

    /**
     * JDBC ResultSet.
     */
    private ResultSet resultSet;

    /**
     * Flag to control if the result set has a next element
     */
    private boolean hasNext = false;

    /**
     * Instantiaties a new JdbcReader.
     * 
     * @param config
     *            JDBC Deep Job configuration.
     */
    public JdbcReader(JdbcDeepJobConfig config) {
        this.jdbcDeepJobConfig = config;
    }

    /**
     * Initialized the reader
     * 
     * @param p
     *            Spark partition.
     * @throws Exception
     */
    public void init(Partition p) throws Exception {
        Class.forName(jdbcDeepJobConfig.getDriverClass());
        conn = DriverManager.getConnection(jdbcDeepJobConfig.getConnectionUrl(),
                jdbcDeepJobConfig.getUsername(),
                jdbcDeepJobConfig.getPassword());
        Statement statement = conn.createStatement();
        SelectQuery query = jdbcDeepJobConfig.getQuery();
        JdbcPartition jdbcPartition = (JdbcPartition)p;
        if(jdbcDeepJobConfig.getNumPartitions() > 1) {
            Column partitionKey = jdbcDeepJobConfig.getPartitionKey();
            query.getWhereClause().addCondition(BinaryCondition.lessThan(partitionKey, jdbcPartition.upper(), true))
                    .addCondition(BinaryCondition.greaterThan(partitionKey, jdbcPartition.lower(), true));
        }
        resultSet = statement.executeQuery(query.toString());
        // Fetches first element
        this.hasNext = resultSet.next();
    }

    /**
     * Checks if there are more results.
     * 
     * @return True if there is another result.
     * @throws SQLException
     */
    public boolean hasNext() throws SQLException {
        return hasNext;
    }

    /**
     * Returns the next result row as a Map of column_name:column_value.
     * 
     * @return Next result row.
     * @throws SQLException
     */
    public Map<String, Object> next() throws SQLException {
        Map<String, Object> row = new HashMap<>();
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnsNumber = metadata.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            String columnName = metadata.getColumnName(i);
            row.put(columnName, resultSet.getObject(i));
        }

        this.hasNext = resultSet.next();

        return row;
    }

    /**
     * closes the resultset and the jdbc connection.
     *
     * @throws java.sql.SQLException
     */
    public void close() throws SQLException {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
