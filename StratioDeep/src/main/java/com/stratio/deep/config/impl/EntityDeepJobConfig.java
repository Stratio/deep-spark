package com.stratio.deep.config.impl;

import java.lang.annotation.AnnotationTypeMismatchException;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.entity.IDeepType;

/**
 * Class containing the appropiate configuration for a CassandraRDD.
 * <p/>
 * Remember to call {@link #getConfiguration()} after having configured all the
 * properties.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class EntityDeepJobConfig<T extends IDeepType> extends GenericDeepJobConfig<T> {

    private static final long serialVersionUID = 4490719746563473495L;

    private Class<T> entityClass;

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
}
