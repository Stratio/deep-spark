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

/**
 * Defines the public methods that each Stratio Deep Jdbc configuration object should implement.
 *
 * @param <T>
 */
public interface IJdbcDeepJobConfig<T, S extends DeepJobConfig> {

    /**
     * Sets the database driver class.
     * @param driverClass Database driver class.
     * @return Configuration object.
     */
    S driverClass(Class driverClass);

    /**
     * Gets the database driver class.
     * @return Database driver class.
     */
    String getDriverClass();

    /**
     * Sets the database connection URL.
     * @param connectionUrl Database JDBC connection URL.
     * @return Configuration object.
     */
    S connectionUrl(String connectionUrl);

    /**
     * Gets the database connection URL.
     * @return Database JDBC connection URL.
     */
    String getConnectionUrl();

    /**
     * Sets the database schema.
     * @param database Database schema.
     * @return Configuration object.
     */
    S database(String database);

    /**
     * Returns the database schema.
     * @return Database schema.
     */
    String getDatabase();

    /**
     * Sets the column for applying sort (optional).
     * @param sort Column for sorting.
     * @return Configuration object.
     */
    S sort(String sort);

    /**
     * Gets the column used for sorting.
     * @return Column used for sorting.
     */
    DbColumn getSort();

    /**
     * Sets the column user for partitioning. Must be a numeric value.
     * @param partitionKey Name of the column used for partitioning.
     * @return Configuration object.
     */
    S partitionKey(String partitionKey);

    /**
     * Returns the name of the column used for partitioning.
     * @return Name of the column used for partitioning.
     */
    DbColumn getPartitionKey();

    /**
     * Sets the upper bound used for partitioning.
     * @param upperBound Upper bound for partitioning.
     * @return Configuration object.
     */
    S upperBound(int upperBound);

    /**
     * Returns the upper bound used for partitioning.
     * @return
     */
    int getUpperBound();

    /**
     * Sets the lower bound used for partitioning.
     * @param lowerBound Lower bound used for partitioning.
     * @return Configuration object.
     */
    S lowerBound(int lowerBound);

    /**
     * Returns the lower bound used for partitioning.
     * @return Lower bound user for partitioning.
     */
    int getLowerBound();

    /**
     * Returns the number of partitions.
     * @return Number of partitions.
     */
    S numPartitions(int numPartitions);

    /**
     * Returns the number of partitions.
     * @return Number of partitions.
     */
    int getNumPartitions();

    /**
     * Returns the query to execute.
     * @return Query to execute.
     */
    SelectQuery getQuery();

    /**
     * Check if Sql tables and fields must be quoted.
     * @param quoteSql
     * @return
     */
    S quoteSql(boolean quoteSql);

    /**
     * Returns if SQL tables and fields must be quoted.
     * @return SQL tables and fields quoted or not.
     */
    boolean getQuoteSql();

}
