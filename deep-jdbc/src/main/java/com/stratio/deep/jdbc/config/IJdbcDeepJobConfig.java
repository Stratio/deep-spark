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

/**
 * Defines the public methods that each Stratio Deep Jdbc configuration object should implement.
 *
 * @param <T>
 */
public interface IJdbcDeepJobConfig<T> {

    IJdbcDeepJobConfig database(String database);

    String getDatabase();

    IJdbcDeepJobConfig table(String table);

    String getTable();

    IJdbcDeepJobConfig query(String query);

    String getQuery();

    IJdbcDeepJobConfig username(String username);

    String getUsername();

    IJdbcDeepJobConfig password(String password);

    String getPassword();

    IJdbcDeepJobConfig fieldNames(String [] fields);

    String[] getFieldNames();

    IJdbcDeepJobConfig orderBy(String orderBy);

    String getOrderBy();

    IJdbcDeepJobConfig filterQuery(String filterQuery);

    String getFilterQuery();

}
