package com.stratio.deep.jdbc.utils;

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.utils.AnnotationUtils;
import com.stratio.deep.commons.utils.Utils;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class UtilJdbc {

    private static final Logger LOG = LoggerFactory.getLogger(UtilJdbc.class);

    public static <T> T getObjectFromRow(Class<T> classEntity, Map<String, Object> row, DeepJobConfig<T, JdbcDeepJobConfig<T>> config) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = classEntity.newInstance();
        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);
        Object insert = null;
        for (Field field : fields) {
            Object currentRow = null;
            Method method = null;
            Class<?> classField = field.getType();
            try {
                method = Utils.findSetter(field.getName(), classEntity, field.getType());

                currentRow = row.get(AnnotationUtils.deepFieldName(field));

                if (currentRow != null) {
                    method.invoke(t, insert);
                }
            } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                LOG.error("impossible to create a java object from column:" + field.getName() + " and type:"
                        + field.getType() + " and value:" + t + "; recordReceived:" + currentRow);

                method.invoke(t, Utils.castNumberType(insert, classField.newInstance()));
            }
        }
        return t;
    }

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

    public static Cells getCellsFromObject(Map<String, Object> row, DeepJobConfig<Cells, JdbcDeepJobConfig<Cells>> config) {
        Cells result = new Cells(config.getCatalog());
        for(Map.Entry<String, Object> entry:row.entrySet()) {
            Cell cell = Cell.create(entry.getKey(), entry.getValue());
            result.add(cell);
        }
        return result;
    }

    public static Map<String, Object> getObjectFromCells(Cells cells) {
        Map<String, Object> result = new HashMap<>();
        for(Cell cell:cells.getCells()) {
            result.put(cell.getName(), cell.getValue());
        }
        return result;
    }
}
