package com.stratio.deep.jdbc.config;

import com.healthmarketscience.sqlbuilder.*;
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
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.JDBC_QUOTE_SQL;

/**
 * Created by mariomgal on 20/01/15.
 */
public class JdbcDeepJobConfig<T> extends DeepJobConfig<T, JdbcDeepJobConfig<T>>
        implements IJdbcDeepJobConfig<T, JdbcDeepJobConfig<T>>, Serializable{

    private static final long serialVersionUID = -3772553194634727039L;

    private Class driverClass;

    private String connectionUrl;

    private transient DbSpec dbSpec = new DbSpec();

    private transient SelectQuery query;

    private transient DbSchema dbSchema;

    private transient DbTable dbTable;

    private DbColumn sort;

    private int upperBound = Integer.MAX_VALUE;

    private int lowerBound = 0;

    private int numPartitions = 1;

    private boolean quoteSql;

    public JdbcDeepJobConfig() {

    }

    public JdbcDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
        if (Cells.class.isAssignableFrom(entityClass)) {
            extractorImplClass = JdbcNativeCellExtractor.class;
        } else {
            extractorImplClass = JdbcNativeEntityExtractor.class;
        }
    }

    @Override
    public JdbcDeepJobConfig<T> initialize() throws IllegalStateException {
        this.validate();
        return this;
    }

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
            driverClass(extractorConfig.getClass(JDBC_DRIVER_CLASS));

        }

        if (values.get(JDBC_CONNECTION_URL) != null) {
            connectionUrl(extractorConfig.getString(JDBC_CONNECTION_URL));
        }

        if (values.get(JDBC_QUOTE_SQL) != null) {
            quoteSql(extractorConfig.getBoolean(JDBC_QUOTE_SQL));
        }
        this.initialize();

        return this;
    }

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
    }

    @Override
    public JdbcDeepJobConfig<T> filters(Filter[] filters) {
        this.filters = filters;
        return this;
    }

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

    @Override
    public JdbcDeepJobConfig<T> connectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Override
    public String getConnectionUrl() {
        if(connectionUrl == null || connectionUrl.isEmpty()) {
            return getJdbcUrl();
        }
        return this.connectionUrl;
    }

    @Override
    public JdbcDeepJobConfig<T> driverClass(Class driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    @Override
    public String getDriverClass() {
        return this.driverClass.getCanonicalName();
    }

    @Override
    public JdbcDeepJobConfig<T> database(String database) {
        this.catalog = database;
        dbSchema = new DbSchema(dbSpec, catalog);
        return this;
    }

    @Override
    public String getDatabase() {
        return this.catalog;
    }

    @Override
    public JdbcDeepJobConfig<T> table(String table) {
        this.table = table;
        if(dbSchema != null) {
            dbTable = new DbTable(dbSchema, table, table);
        }
        return this;
    }

    @Override
    public JdbcDeepJobConfig<T> inputColumns(String... columns) {
        if(dbTable != null) {
            for(String column:columns) {
                dbTable.addColumn(column);
            }
        }
        return this;
    }

    @Override
    public JdbcDeepJobConfig<T> sort(String sort) {
        if(dbTable != null) {
            this.sort = new DbColumn(dbTable, sort, "");
        }
        return this;
    }

    @Override
    public DbColumn getSort() {
        return this.sort;
    }

    @Override
    public JdbcDeepJobConfig<T> upperBound(int upperBound) {
        this.upperBound = upperBound;
        return this;
    }

    @Override
    public int getUpperBound() {
        return this.upperBound;
    }

    @Override
    public JdbcDeepJobConfig<T> lowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
        return this;
    }

    @Override
    public int getLowerBound() {
        return this.lowerBound;
    }

    @Override
    public JdbcDeepJobConfig<T> numPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    @Override
    public int getNumPartitions() {
        return this.numPartitions;
    }

    @Override
    public JdbcDeepJobConfig<T> quoteSql(boolean quoteSql) {
        this.quoteSql = quoteSql;
        return this;
    }

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
                    DbColumn filterColumn = new DbColumn(dbTable, filter.getField(), "");
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
                    } else {
                        throw new UnsupportedOperationException("Currently, the filter operation " + filterType + " is not supported");
                    }
                }
            }
            query.addCondition(comboCondition);
        }
    }

}
