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

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Class with factory method for instantiating JdbcNeo4JDeepJobConfig objects.
 */
public class JdbcNeo4JConfigFactory {

    private JdbcNeo4JConfigFactory() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a new cell-based Jdbc for Neo4J job configuration object.
     *
     * @return a new cell-based Jdbc for Neo4J job configuration object.
     */
    public static JdbcNeo4JDeepJobConfig<Cells> createJdbcNeo4J() {
        return new JdbcNeo4JDeepJobConfig(Cells.class);
    }

    /**
     * Creates a new entity-based Jdbc job configuration object.
     *
     * @param entityClass the class instance of the entity class that will be used to map db objects to Java objects.
     * @param <T>         the generic type of the entity object implementing IDeepType.
     * @return a new entity-based Jdbc job configuration object.
     */
    public static <T extends IDeepType> JdbcNeo4JDeepJobConfig<T> createJdbcNeo4J(Class<T> entityClass) {
        return new JdbcNeo4JDeepJobConfig(entityClass);
    }

}
