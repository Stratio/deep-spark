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
