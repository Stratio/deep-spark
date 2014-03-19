package com.stratio.deep.config.impl;

import java.lang.annotation.AnnotationTypeMismatchException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.utils.AnnotationUtils;

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
            throw new IllegalArgumentException("entity class cannot be null");
        }

        if (!entityClass.isAnnotationPresent(DeepEntity.class)) {
            throw new AnnotationTypeMismatchException(null, entityClass.getCanonicalName());
        }

        super.validate();
    }

    public void setInstancePropertyFromDbName(T instance, String dbName, Object value) {
        Method setter;

        Map<String, Cell> cfs = columnDefinitions();
        Cell metadataCell = cfs.get(dbName);

        String f = mapDBNameToEntityName.get(dbName);

        if (StringUtils.isEmpty(f)) {
            // DB column is not mapped in the entity

            return;
        }

        String setterName = "set" + f.substring(0, 1).toUpperCase() +
            f.substring(1);

        try {

            setter = entityClass.getMethod(setterName, metadataCell.getValueType());
        } catch (NoSuchMethodException e) {
            throw new DeepIOException(e);
        }

        if (setter == null) {
            throw new DeepNoSuchFieldException("Cannot find setter for property: " + dbName);
        }

        try {
            setter.invoke(instance, value);
        } catch (Exception e) {
            throw new DeepGenericException(e);
        }
    }
}
