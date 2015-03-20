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

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.ComboCondition;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.Column;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor;
import com.stratio.deep.jdbc.extractor.JdbcNativeEntityExtractor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.*;

/**
 * Configuration class for Jdbc-Spark integration
 *
 * @param <T> Type of returned objects.
 */
public class JdbcDeepJobConfig<T> extends DeepJobConfig<T, JdbcDeepJobConfig<T>>
        implements IJdbcDeepJobConfig<T, JdbcDeepJobConfig<T>>, Serializable{

    private static final long serialVersionUID = -3772553194634727039L;

    /**
     * JDBC Driver class.
     */
    private Class driverClass;

    /**
     * JDBC connection url.
     */
    private String connectionUrl;

    /**
     * Database root object.
     */
    private transient DbSpec dbSpec = new DbSpec();

    /**
     * Query to be executed.
     */
    private transient SelectQuery query;

    /**
     * Database schema.
     */
    private transient DbSchema dbSchema;

    /**
     * Database table.
     */
    private transient DbTable dbTable;

    /**
     * Column used for sorting (optional).
     */
    private transient DbColumn sort;

    /**
     * Column used for partitioning (must be numeric).
     */
    private transient DbColumn partitionKey;

    /**
     * Partitioning upper bound.
     */
    private int upperBound = Integer.MAX_VALUE - 1;

    /**
     * Partitioning lower bound.
     */
    private int lowerBound = 0;

    /**
     * Number of partitions.
     */
    private int numPartitions = 1;

    /**
     * Quote or not schema and tables names.
     */
    private boolean quoteSql;

    /**
     * Default constructor.
     */
    public JdbcDeepJobConfig() {

    }

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
    @Override
    public JdbcDeepJobConfig<T> initialize() throws IllegalStateException {
        this.validate();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {
        Map<String, Serializable> values = extractorConfig.getValues();
        if (values.get(CATALOG) != null) {
            this.database(extractorConfig.getString(CATALOG));
        }

        if (values.get(TABLE) != null) {
            this.table(extractorConfig.getString(TABLE));
        }
        super.initialize(extractorConfig);
        if (values.get(FILTER_QUERY) != null) {
            this.filters(extractorConfig.getFilterArray(FILTER_QUERY));
        }

        if (values.get(JDBC_DRIVER_CLASS) != null) {
           driverClass(extractorConfig.getString(JDBC_DRIVER_CLASS));

        }

        if (values.get(JDBC_CONNECTION_URL) != null) {
            connectionUrl(extractorConfig.getString(JDBC_CONNECTION_URL));
        }

        if (values.get(JDBC_QUOTE_SQL) != null) {
            quoteSql(extractorConfig.getBoolean(JDBC_QUOTE_SQL));
        }
        if (values.get(JDBC_PARTITION_KEY) != null) {
            partitionKey(extractorConfig.getString(JDBC_PARTITION_KEY));
        }

        if (values.get(JDBC_NUM_PARTITIONS) != null) {
            numPartitions(extractorConfig.getInteger(JDBC_NUM_PARTITIONS));
        }

        if (values.get(JDBC_PARTITIONS_LOWER_BOUND) != null) {
            lowerBound(extractorConfig.getInteger(JDBC_PARTITIONS_LOWER_BOUND));
        }

        if (values.get(JDBC_PARTITIONS_UPPER_BOUND) != null) {
            upperBound(extractorConfig.getInteger(JDBC_PARTITIONS_UPPER_BOUND));
        }
        this.initialize();

        return this;
    }

    /**
     * Validates configuration object.
     */
    private void validate() {
        if(driverClass == null) {
            throw new IllegalArgumentException("Driver class must be specified");
        }
        if(catalog == null || catalog.isEmpty()) {
            throw new IllegalArgumentException("Schema name must be specified");
        }
        if(table == null || table.isEmpty()) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        if(connectionUrl == null || connectionUrl.isEmpty()) {
            if((host != null && !host.isEmpty()) && (port > 0)) {
                connectionUrl(getJdbcUrl());
            } else {
                throw new IllegalArgumentException("You must specify at least one of connectionUrl or host and port properties");
            }
        }
        if(partitionKey == null && numPartitions > 1) {
            throw new IllegalArgumentException("You must define a valid partition key for using more than one partition.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> filters(Filter[] filters) {
        this.filters = filters;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SelectQuery getQuery() {
        SelectQuery selectQuery = new SelectQuery();
        List<DbColumn> columns = dbTable.getColumns();
        if(!columns.isEmpty()) {
            selectQuery.addColumns(columns.toArray(new Column[columns.size()]));
        } else {
            selectQuery.addAllTableColumns(dbTable);
        }
        selectQuery.addFromTable(dbTable);
        if(sort != null) {
            selectQuery.addOrderings(sort);
        }
        applyFilters(selectQuery);
        query = selectQuery;
        return query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> connectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConnectionUrl() {
        if(connectionUrl == null || connectionUrl.isEmpty()) {
            return getJdbcUrl();
        }
        return this.connectionUrl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> driverClass(String driverClass) {
        try {
            this.driverClass = Class.forName(driverClass);
        } catch(ClassNotFoundException e) {
            throw new DeepGenericException("Class " + driverClass + "not found", e);
        }
        return this;
    }

    @Override
    public String getDriverClass() {
        return this.driverClass.getCanonicalName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> database(String database) {
        this.catalog = database;
        dbSchema = new DbSchema(dbSpec, catalog);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabase() {
        return this.catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> table(String table) {
        this.table = table;
        if(dbSchema != null) {
            dbTable = new DbTable(dbSchema, table, table);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> inputColumns(String... columns) {
        if(dbTable != null) {
            for(String column:columns) {
                dbTable.addColumn(column);
            }
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> sort(String sort) {
        if(dbTable != null) {
            this.sort = new DbColumn(dbTable, sort, "",null,null);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbColumn getSort() {
        return this.sort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> partitionKey(String partitionKey) {
        if(dbTable != null) {
            this.partitionKey = new DbColumn(dbTable, partitionKey, "",null,null);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbColumn getPartitionKey() {
        return this.partitionKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcDeepJobConfig<T> upperBound(int upperBound) {
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
    public JdbcDeepJobConfig<T> lowerBound(int lowerBound) {
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
    public JdbcDeepJobConfig<T> numPartitions(int numPartitions) {
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
    public JdbcDeepJobConfig<T> quoteSql(boolean quoteSql) {
        this.quoteSql = quoteSql;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getQuoteSql() {
        return this.quoteSql;
    }

    private String getJdbcUrl() {
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

    private String getJdbcProvider(){
        int firstIndex = driverClass.toString().indexOf(".");
        int secondIndex = driverClass.toString().indexOf(".", ++firstIndex);
        return driverClass.toString().substring(firstIndex, secondIndex);
    }

    private void applyFilters(SelectQuery query) {
        if(this.filters != null && this.filters.length > 0) {
            ComboCondition comboCondition = new ComboCondition(ComboCondition.Op.AND);
            if (filters.length > 0) {
                for(int i=0; i<filters.length; i++) {
                    Filter filter = filters[i];
                    FilterType filterType = filter.getFilterType();
                    DbColumn filterColumn = new DbColumn(dbTable, filter.getField(), "",null,null);
                    if(filterType.equals(FilterType.EQ)) {
                        comboCondition.addCondition(BinaryCondition.equalTo(filterColumn, filter.getValue()));
                    } else if(filterType.equals(FilterType.GT)) {
                        comboCondition.addCondition(BinaryCondition.greaterThan(filterColumn, filter.getValue(), false));
                    } else if(filterType.equals(FilterType.LT)) {
                        comboCondition.addCondition(BinaryCondition.lessThan(filterColumn, filter.getValue(), false));
                    } else if(filterType.equals(FilterType.GTE)) {
                        comboCondition.addCondition(BinaryCondition.greaterThan(filterColumn, filter.getValue(), true));
                    } else if(filterType.equals(FilterType.LTE)) {
                        comboCondition.addCondition(BinaryCondition.lessThan(filterColumn, filter.getValue(), true));
                    } else if(filterType.equals(FilterType.NEQ)) {
                        comboCondition.addCondition(BinaryCondition.notEqualTo(filterColumn, filter.getValue()));
                    } else if(filterType.equals(FilterType.IN)) {
                        ComboCondition comboConditionOR = new ComboCondition(ComboCondition.Op.OR);
                        String[] condicion =filter.getValue().toString().split(",");
                        for (int z=0; z < condicion.length ; z++) {
                            comboConditionOR.addCondition(BinaryCondition.equalTo(filterColumn, condicion[z]));
                        }
                        comboCondition.addCondition(comboConditionOR);
                    }
                    else {
                        throw new UnsupportedOperationException("Currently, the filter operation " + filterType + " is not supported");
                    }
                }
            }
            query.addCondition(comboCondition);
        }
    }

}
