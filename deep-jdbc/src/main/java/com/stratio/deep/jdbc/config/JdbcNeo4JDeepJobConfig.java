
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
package com.stratio.deep.jdbc.config;

import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.jdbc.extractor.JdbcNeo4JNativeCellExtractor;
import com.stratio.deep.jdbc.extractor.JdbcNeo4JNativeEntityExtractor;
import org.neo4j.jdbc.Driver;

import java.io.Serializable;
import java.util.Map;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.JDBC_CONNECTION_URL;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.JDBC_QUERY;

/**
 * Configuration class for Jdbc Neo4J-Spark integration.
 *
 * @param <T> Type of returned objects.
 */
public class JdbcNeo4JDeepJobConfig<T> extends DeepJobConfig<T, JdbcNeo4JDeepJobConfig<T>>
        implements IJdbcDeepJobConfig<T, JdbcNeo4JDeepJobConfig<T>>, Serializable {

    private static final long serialVersionUID = 2175797074628634327L;

    /**
     * Jdbc connection URL.
     */
    private String connectionUrl;

    /**
     * Neo4J query expressed in Cypher language.
     */
    private String cypherQuery;

    /**
     * Default JDBC Neo4J partition lower bound.
     */
    private int lowerBound = 0;

    /**
     * Default JDBC Neo4J partition upper bound.
     */
    private int upperBound = Integer.MAX_VALUE - 1;

    /**
     * Default JDBC Neo4J number of partitions.
     */
    private int numPartitions = 1;

    /**
     * Default public constructor.
     */
    public JdbcNeo4JDeepJobConfig() {

    }

    /**
     * Constructor for entity class-based configuration.
     * @param entityClass
     */
    public JdbcNeo4JDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
        if (Cells.class.isAssignableFrom(entityClass)) {
            extractorImplClass = JdbcNeo4JNativeCellExtractor.class;
        } else {
            extractorImplClass = JdbcNeo4JNativeEntityExtractor.class;
        }
    }

    /**
     * {@inheritDoc}
     */
    public JdbcNeo4JDeepJobConfig<T> initialize() {
        this.validate();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public JdbcNeo4JDeepJobConfig<T> initialize(ExtractorConfig config) {
        Map<String, Serializable> values = config.getValues();
        if(values.get(JDBC_CONNECTION_URL) != null) {
            connectionUrl(config.getString(JDBC_CONNECTION_URL));
        }
        if(values.get(JDBC_QUERY) != null) {
            cypherQuery(config.getString(JDBC_QUERY));
        }
        super.initialize(config);
        return this;
    }

    private void validate() {
        if(connectionUrl == null || connectionUrl.isEmpty()) {
            throw new IllegalArgumentException("You must specify at least one of connectionUrl or host and port properties");
        }
        if(cypherQuery == null || cypherQuery.isEmpty()) {
            throw new IllegalArgumentException("You must specify the Cypher query to execute.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> driverClass(Class driverClass) {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverClass() {
        return Driver.class.getCanonicalName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> connectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConnectionUrl() {
        return this.connectionUrl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> database(String database) {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabase() {
        return "NEO4J";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> sort(String sort) {
        throw new UnsupportedOperationException("Cannot configure sort for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbColumn getSort() {
        throw new UnsupportedOperationException("Cannot configure sort for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> partitionKey(String partitionKey) {
        throw new UnsupportedOperationException("Cannot configure partitioning for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbColumn getPartitionKey() {
        throw new UnsupportedOperationException("Cannot configure partitioning for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> upperBound(int upperBound) {
        throw new UnsupportedOperationException("Cannot configure partitioning for Neo4J extractor");
    }

    /**
     * Sets the cypher query to be executed.
     * @param cypherQuery Query to be executed, expressed in Cypher language.
     * @return Configuration object.
     */
    public JdbcNeo4JDeepJobConfig<T> cypherQuery(String cypherQuery) {
        this.cypherQuery = cypherQuery;
        return this;
    }

    /**
     * Returns the Cypher query to be executed.
     * @return Query to be executed, expressed in Cypher language.
     */
    public String getCypherQuery() {
        return this.cypherQuery;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLowerBound() {
        return lowerBound;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> numPartitions(int numPartitions) {
        throw new UnsupportedOperationException("Cannot configure partitioning for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SelectQuery getQuery() {
        throw new UnsupportedOperationException("Cannot configure SQL query for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> quoteSql(boolean quoteSql) {
        throw new UnsupportedOperationException("Cannot configure SQL formatfor Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getQuoteSql() {
        throw new UnsupportedOperationException("Cannot configure SQL format for Neo4J extractor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpperBound() {
        return upperBound;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcNeo4JDeepJobConfig<T> lowerBound(int lowerBound) {
        throw new UnsupportedOperationException("Cannot configure partitioning for Neo4J extractor");
    }

}
