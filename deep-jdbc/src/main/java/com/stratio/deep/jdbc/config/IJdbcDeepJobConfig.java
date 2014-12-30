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

/**
 * Defines the public methods that each Stratio Deep Jdbc configuration object should implement.
 *
 * @param <T>
 */
public interface IJdbcDeepJobConfig<T, S extends DeepJobConfig> {

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
     * Sets the query to execute.
     * @param query Query to execute.
     * @return Configuration object.
     */
    S query(String query);

    /**
     * Returns the query to execute.
     * @return Query to execute.
     */
    String getQuery();

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
    int getNumPartitions();

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
