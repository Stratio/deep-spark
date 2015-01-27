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

import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import org.apache.spark.Partition;
import org.neo4j.jdbc.Driver;

import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Creates a new JDBC connection to Neo4J and provides methods for reading from it.
 */
public class JdbcNeo4JReader extends JdbcReader implements IJdbcReader {

    /**
     * JDBC Neo4J deep configuration.
     */
    private JdbcNeo4JDeepJobConfig jdbcNeo4JDeepJobConfig;

    /**
     * Instantiates a new JDBCNeo4JReader
     * @param config Deep configuration object.
     */
    public JdbcNeo4JReader(JdbcNeo4JDeepJobConfig config) {
        this.jdbcNeo4JDeepJobConfig = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(Partition p) throws Exception {
        Class.forName(Driver.class.getCanonicalName());
        conn = DriverManager.getConnection(jdbcNeo4JDeepJobConfig.getConnectionUrl(),
                jdbcNeo4JDeepJobConfig.getUsername(),
                jdbcNeo4JDeepJobConfig.getPassword());
        Statement statement = conn.createStatement();
        String query = jdbcNeo4JDeepJobConfig.getCypherQuery();
        resultSet = statement.executeQuery(query);
        // Fetches first element
        this.hasNext = resultSet.next();
    }

}
