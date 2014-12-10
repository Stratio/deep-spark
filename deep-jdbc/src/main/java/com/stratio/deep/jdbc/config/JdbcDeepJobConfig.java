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

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.FILTER_QUERY;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.CATALOG;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.TABLE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.FILTER_FIELD;

/**
 * Configuration class for Jdbc-Spark integration
 *
 * @param <T>
 */
public class JdbcDeepJobConfig<T> extends HadoopConfig<T, JdbcDeepJobConfig<T>>  implements
        IJdbcDeepJobConfig<T>, Serializable {

    private static final long serialVersionUID = -6487437723098215934L;

    private String driverClass;

    private String database;

    private String table;

    private String username;

    private String password;

    private String query;

    private String orderBy;

    private String [] fieldNames;

    private String filterQuery;


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

    public JdbcDeepJobConfig driverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    public String getDriverClass() {
        return this.driverClass;
    }

    @Override
    public JdbcDeepJobConfig database(String database) {
        this.database = database;
        return this;
    }

    @Override
    public String getDatabase() {
        return this.database;
    }

    @Override
    public String getTable() {
        return this.table;
    }

    @Override
    public JdbcDeepJobConfig table(String table) {
        this.table = table;
        return this;
    }

    public String getJdbcUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc://");
        sb.append(host);
        sb.append(":");
        sb.append(port);
        sb.append("/");
        sb.append(catalog);
        return sb.toString();
    }

    @Override
    public JdbcDeepJobConfig query(String query) {
        this.query = query;
        return this;
    }

    @Override
    public String getQuery() {
        return this.query;
    }

    @Override
    public JdbcDeepJobConfig<T> username(String username) {
        this.username = username;
        return this;
    }

    @Override
    public String getUsername() {
        return this.username;
    }

    @Override
    public JdbcDeepJobConfig<T> password(String password) {
        this.password = password;
        return this;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public JdbcDeepJobConfig fieldNames(String [] fields) {
        this.fieldNames = fields;
        return this;
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public JdbcDeepJobConfig orderBy(String orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    @Override
    public String getOrderBy() {
        return this.orderBy;
    }

    @Override
    public JdbcDeepJobConfig filterQuery(String filterQuery) {
        this.filterQuery = filterQuery;
        return this;
    }

    @Override
    public String getFilterQuery() {
        return this.filterQuery;
    }

    @Override
    public JdbcDeepJobConfig<T> initialize() throws IllegalStateException {
        validate();
        configHadoop = new Configuration();
        DBConfiguration.configureDB(configHadoop,
                                    driverClass,
                                    getJdbcUrl(),
                                    username,
                                    password);
        configHadoop.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, table);
        configHadoop.set(DBConfiguration.INPUT_QUERY, query);
        configHadoop.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, table);
        if(fieldNames != null) {
            configHadoop.set(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames.toString());
            configHadoop.set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, fieldNames.toString());
        }
        if(!orderBy.isEmpty()) {
            configHadoop.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderBy);
        }
        if(!filterQuery.isEmpty()) {
            configHadoop.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, filterQuery);
        }
        return this;
    }

    private void validate() {
        if(host.isEmpty()) {
            throw new IllegalArgumentException("Host must be specified");
        }
        if(port <= 0) {
            throw new IllegalArgumentException("Port must be valid");
        }
        if(driverClass.isEmpty()) {
            throw new IllegalArgumentException("Driver class must be specified");
        }
        if(catalog.isEmpty()) {
            throw new IllegalArgumentException("Schema name must be specified");
        }
        if(table.isEmpty()) {
            throw new IllegalArgumentException("Table name must be specified");
        }
    }

    @Override
    public Configuration getHadoopConfiguration() {
        if (configHadoop == null) {
            initialize();
        }
        return configHadoop;
    }

    @Override
    public JdbcDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {
        super.initialize(extractorConfig);

        Map<String, Serializable> values = extractorConfig.getValues();

        if (values.get(CATALOG) != null) {
            catalog(extractorConfig.getString(CATALOG));
        }

        if (values.get(TABLE) != null) {
            table(extractorConfig.getString(TABLE));
        }

        if (values.get(FILTER_QUERY) != null) {
            filterQuery(extractorConfig.getFilterArray(FILTER_QUERY));
        }

        this.initialize();

        return this;
    }

    private JdbcDeepJobConfig<T> filterQuery(Filter[] filters) {
        StringBuilder sb = new StringBuilder();
        if (filters.length > 0) {
            for(int i=0; i<filters.length; i++) {
                Filter filter = filters[i];
                sb.append(filter.getField());
                sb.append(getSqlOperatorFromFilter(filter));
                sb.append(filter.getValue());
                if(i < (filters.length - 1)) {
                    sb.append(" AND ");
                }
            }
        }
        filterQuery(sb.toString());
        return this;
    }

    private String getSqlOperatorFromFilter(Filter filter) {
        FilterType type = filter.getFilterType();
        if(FilterType.EQ.equals(type)) {
            return "=";
        } else if(FilterType.GT.equals(type)) {
            return ">";
        } else if(FilterType.GTE.equals(type)) {
            return ">=";
        } else if(FilterType.LT.equals(type)) {
            return "<";
        } else if(FilterType.LTE.equals(type)) {
            return "<=";
        } else if(FilterType.NEQ.equals(type)) {
            return "!=";
        } else if(FilterType.IN.equals(type)) {
            return "IN";
        }
        throw new UnsupportedOperationException();
    }
}
