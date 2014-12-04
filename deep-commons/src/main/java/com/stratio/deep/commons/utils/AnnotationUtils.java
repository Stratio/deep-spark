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

package com.stratio.deep.commons.utils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;

import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepIOException;

/**
 * Common utility methods to manipulate beans and fields annotated with @DeepEntity and @DeepField.
 */
public final class AnnotationUtils {

    /**
     * Returns the field name as known by the datastore. If the provided field object DeepField annotation
     * specifies the fieldName property, the value of this property will be returned, otherwise the java field name
     * will be returned.
     *
     * @param field the Field object associated to the property for which we want to resolve the name.
     * @return the field name.
     */
    public static String deepFieldName(Field field) {

        DeepField annotation = field.getAnnotation(DeepField.class);
        if (StringUtils.isNotEmpty(annotation.fieldName())) {
            return annotation.fieldName();
        } else {
            return field.getName();
        }
    }

    /**
     * Utility method that filters out all the fields _not_ annotated
     * with the {@link com.stratio.deep.commons.annotations.DeepField} annotation.
     *
     * @param clazz the Class object for which we want to resolve deep fields.
     * @return an array of deep Field(s).
     */
    public static Field[] filterDeepFields(Class clazz) {
        Field[] fields = Utils.getAllFields(clazz);
        List<Field> filtered = new ArrayList<>();
        for (Field f : fields) {
            if (f.isAnnotationPresent(DeepField.class)) {
                filtered.add(f);
            }
        }
        return filtered.toArray(new Field[filtered.size()]);
    }

    /**
     * Return a pair of Field[] whose left element is
     * the array of keys fields.
     * The right element contains the array of all other non-key fields.
     *
     * @param clazz the Class object
     * @return a pair object whose first element contains key fields, and whose second element contains all other columns.
     */
    public static Pair<Field[], Field[]> filterKeyFields(Class clazz) {
        Field[] filtered = filterDeepFields(clazz);
        List<Field> keys = new ArrayList<>();
        List<Field> others = new ArrayList<>();

        for (Field field : filtered) {
            if (isKey(field.getAnnotation(DeepField.class))) {
                keys.add(field);
            } else {
                others.add(field);
            }
        }

        return Pair.create(keys.toArray(new Field[keys.size()]), others.toArray(new Field[others.size()]));
    }

    /**
     * Returns true is given field is part of the table key.
     *
     * @param field the Field object we want to process.
     * @return true if the field is part of the cluster key or the partition key, false otherwise.
     */
    public static boolean isKey(DeepField field) {
        return field.isPartOfClusterKey() || field.isPartOfPartitionKey();
    }

    /**
     * Returns the value of the fields <i>deepField</i> in the instance <i>entity</i> of type T.
     *
     * @param entity    the entity to process.
     * @param deepField the Field to process belonging to <i>entity</i>
     * @return the property value.
     */
    public static Serializable getBeanFieldValue(IDeepType entity, Field deepField) {
        try {
            return (Serializable) PropertyUtils.getProperty(entity, deepField.getName());

        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e1) {
            throw new DeepIOException(e1);
        }

    }

    /**
     * Returns the list of generic types associated to the provided field (if any).
     *
     * @param field the field instance to process.
     * @return the list of generic types associated to the provided field (if any).
     */
    public static Class[] getGenericTypes(Field field) {
        try {
            ParameterizedType type = (ParameterizedType) field.getGenericType();
            Type[] types = type.getActualTypeArguments();

            Class<?>[] res = new Class<?>[types.length];

            for (int i = 0; i < types.length; i++) {
                res[i] = (Class<?>) types[i];
            }

            return res;

        } catch (ClassCastException e) {
            return new Class<?>[] { (Class<?>) field.getGenericType() };
        }
    }

    /**
     * private constructor.
     */
    private AnnotationUtils() {
    }
}
