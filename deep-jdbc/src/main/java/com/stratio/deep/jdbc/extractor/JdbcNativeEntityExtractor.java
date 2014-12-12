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
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.utils.UtilJdbc;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcNativeEntityExtractor<T> extends JdbcNativeExtractor<T, JdbcDeepJobConfig<T>> {

    private static final long serialVersionUID = 957463022436044036L;

    public JdbcNativeEntityExtractor(Class<T> t) {
        this.jdbcDeepJobConfig = new JdbcDeepJobConfig<>(t);
    }

    @Override
    protected T transformElement(Map<String, Object> entity) {
        try {
            return (T)UtilJdbc.getObjectFromRow(jdbcDeepJobConfig.getEntityClass(), entity, jdbcDeepJobConfig);
        } catch(IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new DeepTransformException(e);
        }
    }

    @Override
    protected Map<String, Object> transformElement(T entity) {
        try {
            return UtilJdbc.getRowFromObject(entity);
        } catch(IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new DeepTransformException(e);
        }
    }

}
