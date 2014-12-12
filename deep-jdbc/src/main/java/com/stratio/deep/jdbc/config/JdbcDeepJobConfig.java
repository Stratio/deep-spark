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

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.config.HadoopConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor;
import com.stratio.deep.jdbc.extractor.JdbcNativeEntityExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

import java.io.Serializable;
import java.util.Map;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.*;

/**
 * Configuration class for Jdbc-Spark integration
 *
 * @param <T> Type of returned objects.
 */
public class JdbcDeepJobConfig<T> extends DeepJobConfig<T, JdbcDeepJobConfig<T>> implements
        IJdbcDeepJobConfig<T, JdbcDeepJobConfig<T>>, Serializable {

    private static final long serialVersionUID = -6487437723098215934L;

    /**
     * JDBC driver class name.
     */
    private String driverClass;

    /**
     * Query to be executed.
     */
    private String query;

    /**
     * Upper bound used for partitioning.
     */
    private int upperBound = Integer.MAX_VALUE;

    /**
     * Lower bound used for partitioning.
     */
    private int lowerBound = 0;

    /**
     * Number of partitions.
     */
    private int numPartitions = 1;


    /**
     * Constructor for Entity class-based configuration.
     *
     * @param entityClass
     */
    public JdbcDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
        if (Cells.class.isAssignableFrom(entityClass)) {
            extractorImplClass = JdbcNativeCellExtractor.class;
        } else {
            extractorImplClass = JdbcNativeEntityExtractor.class;
        }
    }

    /**
     * {@inheritDoc}
     */
    public JdbcDeepJobConfig driverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public String getDriverClass() {
        return this.driverClass;
    }

    /**
     * Builds the JDBC url from the configuration
     * @return JDBC connection url.
     */
    public String getJdbcUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:");
        sb.append(getJdbcProvider());
        sb.append("://");
        sb.append(host.get(0));
        sb.append(":");
        sb.append(port);
        sb.append("/");
        sb.append(catalog);
        sb.append("?");
        return sb.toString();
    }

    /**
     * Extracts the JDBC provider name from the driver class name.
     * @return JDBC provider name.
     */
    private String getJdbcProvider(){
        int firstIndex = driverClass.indexOf(".");
        int secondIndex = driverClass.indexOf(".", ++firstIndex);
        return driverClass.substring(firstIndex, secondIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig username(String username) {
        this.username = username;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig password(String password) {
        this.password = password;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig database(String database) {
        this.catalog = database;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabase() {
        return catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig table(String table) {
        this.table = table;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTable() {
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig query(String query) {
        this.query = query;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getQuery() {
        return this.query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig upperBound(int upperBound) {
        this.upperBound = upperBound;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpperBound() {
        return this.upperBound;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig lowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLowerBound() {
        return this.lowerBound;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig numPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumPartitions() {
        return this.numPartitions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> initialize() throws IllegalStateException {
        validate();
        return this;
    }

    private void validate() {
        if(host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host must be specified");
        }
        if(port <= 0) {
            throw new IllegalArgumentException("Port must be valid");
        }
        if(driverClass == null || driverClass.isEmpty()) {
            throw new IllegalArgumentException("Driver class must be specified");
        }
        if(catalog == null || catalog.isEmpty()) {
            throw new IllegalArgumentException("Schema name must be specified");
        }
        if(table == null || table.isEmpty()) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        if(query == null || query.isEmpty()) {
            query("SELECT * FROM " + getTable());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {
        super.initialize(extractorConfig);

        Map<String, Serializable> values = extractorConfig.getValues();

        if (values.get(JDBC_DRIVER_CLASS) != null) {
            driverClass(extractorConfig.getString(JDBC_DRIVER_CLASS));
        }

        if (values.get(JDBC_QUERY) != null) {
            query(extractorConfig.getString(JDBC_QUERY));
        }
        this.initialize();

        return this;
    }

}
