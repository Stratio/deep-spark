package com.stratio.deep.config.impl;

import java.lang.annotation.AnnotationTypeMismatchException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.util.Utils;

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

    private Map<String, Method> mapDBNameToEntityName = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public IDeepJobConfig<T> initialize() {
	super.initialize();

	Field[] deepFields = Utils.filterDeepFields(entityClass.getDeclaredFields());

	for (Field f : deepFields) {
	    String fieldName = Utils.deepFieldName(f);
	    String beanFieldName = f.getName();
	    Method setter;
	    try {
		setter = entityClass.getMethod("set" + beanFieldName.substring(0, 1).toUpperCase() +
				beanFieldName.substring(1), f.getType());
	    } catch (NoSuchMethodException e) {
		throw new DeepIOException(e);
	    }

	    mapDBNameToEntityName.put(fieldName, setter);
	}

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
	super.validate();

	if (entityClass == null) {
	    throw new IllegalArgumentException("entity class cannot be null");
	}

	if (!entityClass.isAnnotationPresent(DeepEntity.class)) {
	    throw new AnnotationTypeMismatchException(null, entityClass.getCanonicalName());
	}
    }

    public void setInstancePropertyFromDbName(T instance, String dbName, Object value) {
	Method m = mapDBNameToEntityName.get(dbName);

	if (m == null) {
	    throw new DeepNoSuchFieldException("Cannot find setter for property: " + dbName);
	}

	try {
	    m.invoke(instance, value);
	} catch (Exception e) {
	    throw new DeepGenericException(e);
	}
    }
}
