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
package com.stratio.deep.jdbc.writer;

import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import org.neo4j.jdbc.Driver;

import java.sql.DriverManager;
import java.util.Map;

/**
 * Helper for write data to Neo4J using a JDBC connection.
 */
public class JdbcNeo4JWriter<T> extends JdbcWriter<T> {

    /**
     * JDBC for Neo4J Stratio Deep configuration object.
     */
    private JdbcNeo4JDeepJobConfig jdbcNeo4JDeepJobConfig;

    /**
     * Instantiates a new JdbcNeo4JWriter using a given configuration.
     * @param config Stratio Deep configuration object.
     * @throws Exception
     */
    public JdbcNeo4JWriter(JdbcNeo4JDeepJobConfig config) throws Exception {
        this.jdbcNeo4JDeepJobConfig = config;
        Class.forName(Driver.class.getCanonicalName());
        this.conn = DriverManager.getConnection(jdbcNeo4JDeepJobConfig.getConnectionUrl(),
                jdbcNeo4JDeepJobConfig.getUsername(),
                jdbcNeo4JDeepJobConfig.getPassword());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(Map<String, Object> row) throws Exception {
        throw new UnsupportedOperationException("Saving not yet supported for Neo4J");
    }

}
