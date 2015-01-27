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

package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import com.stratio.deep.jdbc.utils.UtilJdbc;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Implementation of JdbcNativeExtractor for Stratio Deep Entity objects.
 */
public class JdbcNeo4JNativeEntityExtractor<T> extends JdbcNeo4JNativeExtractor<T, JdbcNeo4JDeepJobConfig> {

    private static final long serialVersionUID = 5259304012388612172L;

    /**
     * Instatiates a new entity extractor.
     * @param entityClass Stratio Deep entity.
     */
    public JdbcNeo4JNativeEntityExtractor(Class<T> entityClass) {
        this.jdbcNeo4JDeepJobConfig = new JdbcNeo4JDeepJobConfig<>(entityClass);
    }

    /**
     * Transforms a database row represented as a Map into a Stratio Deep Entity.
     * @param entity Database row represented as a Map of column_name:column_value.
     * @return Stratio Deep Entity carrying row data.
     */
    @Override
    protected T transformElement(Map<String, Object> entity) {
        try {
            return (T) UtilJdbc.getObjectFromRow(jdbcNeo4JDeepJobConfig.getEntityClass(), entity, jdbcNeo4JDeepJobConfig);
        } catch(IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new DeepTransformException(e);
        }
    }

    /**
     * Trasforms a Stratio Deep Entity into a database row represented as a Map.
     * @param entity Stratio Deep entity.
     * @return Database row represented as a Map of column_name:column_value.
     */
    @Override
    protected Map<String, Object> transformElement(T entity) {
        try {
            return UtilJdbc.getRowFromObject(entity);
        } catch(IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new DeepTransformException(e);
        }
    }
}
