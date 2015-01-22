package com.stratio.deep.jdbc.config;

import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.stratio.deep.commons.config.DeepJobConfig;

/**
 * Created by mariomgal on 21/01/15.
 */
public interface IJdbcDeepJobConfig<T, S extends DeepJobConfig> {

    S driverClass(Class driverClass);

    String getDriverClass();

    S connectionUrl(String connectionUrl);

    String getConnectionUrl();

    S database(String database);

    String getDatabase();

    S sort(String sort);

    DbColumn getSort();

    S upperBound(int upperBound);

    int getUpperBound();

    S lowerBound(int numPartitions);

    int getLowerBound();

    S numPartitions(int numPartitions);

    int getNumPartitions();

    SelectQuery getQuery();

    S quoteSql(boolean quoteSql);

    boolean getQuoteSql();

}
