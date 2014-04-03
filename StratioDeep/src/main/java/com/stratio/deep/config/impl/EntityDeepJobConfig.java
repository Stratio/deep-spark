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

package com.stratio.deep.config.impl;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.utils.AnnotationUtils;
import com.stratio.deep.utils.Utils;
import org.apache.commons.lang.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.annotation.AnnotationTypeMismatchException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Class containing the appropiate configuration for a CassandraEntityRDD.
 * <p/>
 * Remember to call {@link #getConfiguration()} after having configured all the
 * properties.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class EntityDeepJobConfig<T extends IDeepType> extends GenericDeepJobConfig<T> {

    private static final long serialVersionUID = 4490719746563473495L;

    private Class<T> entityClass;

    private Map<String, String> mapDBNameToEntityName;

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> initialize() {
        super.initialize();

        Map<String, String> tmpMap = new HashMap<>();

        Field[] deepFields = AnnotationUtils.filterDeepFields(entityClass.getDeclaredFields());

        for (Field f : deepFields) {
            String dbName = AnnotationUtils.deepFieldName(f);
            String beanFieldName = f.getName();

            tmpMap.put(dbName, beanFieldName);
        }

        mapDBNameToEntityName = Collections.unmodifiableMap(tmpMap);

        return this;
    }

    public EntityDeepJobConfig(Class<T> entityClass) {
        super();
        this.entityClass = entityClass;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.config.IDeepJobConfig#getEntityClass()
     */
    @Override
    public Class<T> getEntityClass() {
        checkInitialized();
        return entityClass;
    }

    /* (non-Javadoc)
       * @see com.stratio.deep.config.IDeepJobConfig#validate()
       */
    @Override
    public void validate() {

        if (entityClass == null) {
            throw new IllegalArgumentException("testentity class cannot be null");
        }

        if (!entityClass.isAnnotationPresent(DeepEntity.class)) {
            throw new AnnotationTypeMismatchException(null, entityClass.getCanonicalName());
        }

        super.validate();

        /* let's validate fieldNames in @DeepField annotations */
        Field[] deepFields = AnnotationUtils.filterDeepFields(entityClass.getDeclaredFields());

        Map<String, Cell> colDefs = super.columnDefinitions();

        /* colDefs is null if table does not exist. I.E. this configuration will be used as an output configuration
         object, and the output table is dynamically created */
        if (colDefs == null){
            return;
        }

        for (Field field : deepFields) {
            Annotation annotation = field.getAnnotation(DeepField.class);
            String annotationFieldName = AnnotationUtils.deepFieldName(field);

            if (!colDefs.containsKey(annotationFieldName)){
                throw new DeepNoSuchFieldException("Unknown column name \'" + annotationFieldName + "\' specified for" +
                    " field " + entityClass.getCanonicalName() + "#" + field.getName() +". Please, " +
                    "make sure the field name you specify in @DeepField annotation matches _exactly_ the column name " +
                    "in the database");
            }
        }
    }

    public void setInstancePropertyFromDbName(T instance, String dbName, Object value) {
        Map<String, Cell> cfs = columnDefinitions();
        Cell metadataCell = cfs.get(dbName);

        String f = mapDBNameToEntityName.get(dbName);

        if (StringUtils.isEmpty(f)) {
            // DB column is not mapped in the testentity
            return;
        }

        Method setter = Utils.findSetter(f, entityClass, metadataCell.getValueType());

        try {
            setter.invoke(instance, packageCollectionValue(metadataCell, value));
        } catch (Exception e) {
            throw new DeepGenericException(e);
        }
    }

    private Object packageCollectionValue(Cell metadataCell, Object value){
        switch (metadataCell.getCellValidator().validatorKind()){
            case SET:
                return new LinkedHashSet((Collection)value);
            case LIST:
                return new LinkedList((Collection)value);
            case MAP:
                return new LinkedHashMap((Map)value);
            default:
                return value;
        }
    }
}
