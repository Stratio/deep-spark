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

package com.stratio.deep.jdbc.utils;

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.utils.AnnotationUtils;
import com.stratio.deep.commons.utils.Utils;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Utils for transforming Jdbc row data structures into Stratio Cells and Entities.
 */
public class UtilJdbc {

    private static final Logger LOG = LoggerFactory.getLogger(UtilJdbc.class);

    /**
     * Returns a Stratio Entity from a Jdbc row represented as a map.
     * @param classEntity Stratio Entity.
     * @param row Jdbc row represented as a Map.
     * @param config JDBC Deep Job configuration.
     * @param <T> Stratio Entity class.
     * @return Stratio Entity from a Jdbc row represented as a map.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> T getObjectFromRow(Class<T> classEntity, Map<String, Object> row, DeepJobConfig<T, JdbcDeepJobConfig<T>> config) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = classEntity.newInstance();
        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);
        for (Field field : fields) {
            Object currentRow = null;
            Method method = null;
            Class<?> classField = field.getType();
            try {
                method = Utils.findSetter(field.getName(), classEntity, field.getType());

                currentRow = row.get(AnnotationUtils.deepFieldName(field));

                if (currentRow != null) {
                    method.invoke(t, currentRow);
                }
            } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                LOG.error("impossible to create a java object from column:" + field.getName() + " and type:"
                        + field.getType() + " and value:" + t + "; recordReceived:" + currentRow);

                method.invoke(t, Utils.castNumberType(currentRow, classField.newInstance()));
            }
        }
        return t;
    }

    /**
     * Returns a JDBC row data structure from a Stratio Deep Entity.
     * @param entity Stratio Deep entity.
     * @param <T> Stratio Deep entity type.
     * @return JDBC row data structure from a Stratio Deep Entity.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> Map<String, Object> getRowFromObject(T entity) throws IllegalAccessException, InstantiationException,
            InvocationTargetException {
        Field[] fields = AnnotationUtils.filterDeepFields(entity.getClass());

        Map<String, Object> row = new HashMap<>();

        for (Field field : fields) {
            Method method = Utils.findGetter(field.getName(), entity.getClass());
            Object object = method.invoke(entity);
            if (object != null) {
                row.put(field.getName(), object);
            }
        }
        return row;
    }

    /**
     * Returns a Cells object from a JDBC row data structure.
     * @param row JDBC row data structure as a Map.
     * @param config JDBC Deep Job config.
     * @return Cells object from a JDBC row data structure.
     */
    public static Cells getCellsFromObject(Map<String, Object> row, DeepJobConfig<Cells, JdbcDeepJobConfig<Cells>> config) {
        Cells result = new Cells(config.getCatalog());
        for(Map.Entry<String, Object> entry:row.entrySet()) {
            Cell cell = Cell.create(entry.getKey(), entry.getValue());
            result.add(cell);
        }
        return result;
    }

    /**
     * Returns a JDBC row data structure from a Cells object.
     * @param cells Cells object carrying information.
     * @return JDBC row data structure from a Cells object.
     */
    public static Map<String, Object> getObjectFromCells(Cells cells) {
        Map<String, Object> result = new HashMap<>();
        for(Cell cell:cells.getCells()) {
            result.put(cell.getName(), cell.getValue());
        }
        return result;
    }
}
