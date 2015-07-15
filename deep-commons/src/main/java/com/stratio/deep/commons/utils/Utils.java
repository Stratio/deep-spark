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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepExtractorInitializationException;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.rdd.IExtractor;

import scala.Tuple2;

/**
 * Utility class providing useful methods to manipulate the conversion
 * between ByteBuffers maps coming from the underlying Cassandra API to
 * instances of a concrete javabean.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class Utils {

    /**
     * Creates a new instance of the given class.
     *
     * @param <T>   the type parameter
     * @param clazz the class object for which a new instance should be created.
     * @return the new instance of class clazz.
     */
    public static <T extends IDeepType> T newTypeInstance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DeepGenericException(e);
        }
    }

    /**
     * Creates a new instance of the given class name.
     *
     * @param <T>         the type parameter
     * @param className   the class object for which a new instance should be created.
     * @param returnClass the return class
     * @return the new instance of class clazz.
     */
    @SuppressWarnings("unchecked")
    public static <T> T newTypeInstance(String className, Class<T> returnClass) {
        try {
            Class<T> clazz = (Class<T>) Class.forName(className);
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DeepGenericException(e);
        }
    }

    /**
     * Quoting for working with uppercase
     *
     * @param identifier the identifier
     * @return the string
     */
    public static String quote(String identifier) {
        if (StringUtils.isEmpty(identifier)) {
            return identifier;
        }

        String res = identifier.trim();

        if (!res.startsWith("\"")) {
            res = "\"" + res;
        }

        if (!res.endsWith("\"")) {
            res = res + "\"";
        }

        return res;

    }

    /**
     * Quoting for working with uppercase
     *
     * @param identifier the identifier
     * @return the string
     */
    public static String singleQuote(String identifier) {
        if (StringUtils.isEmpty(identifier)) {
            return identifier;
        }

        String res = identifier.trim();

        if (!res.startsWith("'")) {
            res = "'" + res;
        }

        if (!res.endsWith("'")) {
            res = res + "'";
        }

        return res;
    }

    /**
     * Returns a CQL batch query wrapping the given statements.
     *
     * @param statements the list of statements to use to generate the batch statement.
     * @return the batch statement.
     */
    public static String batchQueryGenerator(List<String> statements) {
        StringBuilder sb = new StringBuilder("BEGIN BATCH \n");

        for (String statement : statements) {
            sb.append(statement).append("\n");
        }

        sb.append(" APPLY BATCH;");

        return sb.toString();
    }

    /**
     * Splits columns names and values as required by Datastax java driver to generate an Insert query.
     *
     * @param tuple an object containing the key Cell(s) as the first element and all the other columns as the second element.
     * @return an object containing an array of column names as the first element and an array of column values as the second element.
     */
    public static Tuple2<String[], Object[]> prepareTuple4CqlDriver(Tuple2<Cells, Cells> tuple) {
        Cells keys = tuple._1();
        Cells columns = tuple._2();

        String[] names = new String[keys.size() + columns.size()];
        Object[] values = new Object[keys.size() + columns.size()];

        for (int k = 0; k < keys.size(); k++) {
            Cell cell = keys.getCellByIdx(k);

            names[k] = quote(cell.getCellName());
            values[k] = cell.getCellValue();
        }

        for (int v = keys.size(); v < (keys.size() + columns.size()); v++) {
            Cell cell = columns.getCellByIdx(v - keys.size());

            names[v] = quote(cell.getCellName());
            values[v] = cell.getCellValue();
        }

        return new Tuple2<>(names, values);
    }

    /**
     * Resolves the setter name for the property whose name is 'propertyName' whose type is 'valueType'
     * in the entity bean whose class is 'entityClass'.
     * If we don't find a setter following Java's naming conventions, before throwing an exception we try to
     * resolve the setter following Scala's naming conventions.
     *
     * @param propertyName the field name of the property whose setter we want to resolve.
     * @param entityClass  the bean class object in which we want to search for the setter.
     * @param valueType    the class type of the object that we want to pass to the setter.
     * @return the resolved setter.
     */
    @SuppressWarnings("unchecked")
    public static Method findSetter(String propertyName, Class entityClass, Class valueType) {
        Method setter;
        String setterName = "set" + propertyName.substring(0, 1).toUpperCase() +
                propertyName.substring(1);

        try {
            setter = entityClass.getMethod(setterName, valueType);
        } catch (NoSuchMethodException e) {
            // let's try with scala setter name
            try {
                setter = entityClass.getMethod(propertyName + "_$eq", valueType);
            } catch (NoSuchMethodException e1) {
                throw new DeepIOException(e1);
            }
        }

        return setter;
    }

    /**
     * @param object
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static boolean setFieldWithReflection(Object object, String fieldName, Object fieldValue) {
        Class<?> clazz = object.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(object, fieldValue);
                return true;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return false;
    }

    /**
     * Resolves the getter name for the property whose name is 'propertyName' whose type is 'valueType'
     * in the entity bean whose class is 'entityClass'.
     * If we don't find a setter following Java's naming conventions, before throwing an exception we try to
     * resolve the setter following Scala's naming conventions.
     *
     * @param propertyName the field name of the property whose getter we want to resolve.
     * @param entityClass  the bean class object in which we want to search for the getter.
     * @return the resolved getter.
     */
    @SuppressWarnings("unchecked")
    public static Method findGetter(String propertyName, Class entityClass) {
        Method getter;

        String getterName = "get" + propertyName.substring(0, 1).toUpperCase() +
                propertyName.substring(1);
        try {
            getter = entityClass.getMethod(getterName);
        } catch (NoSuchMethodException e) {
            // let's try with scala setter name
            try {
                getter = entityClass.getMethod(propertyName + "_$eq");
            } catch (NoSuchMethodException e1) {
                throw new DeepIOException(e1);
            }
        }

        return getter;
    }


    /**
     * Return the set of fields declared at all level of class hierachy
     *
     * @param clazz the clazz
     * @return the field [ ]
     */
    public static Field[] getAllFields(Class clazz) {
        return getAllFieldsRec(clazz, new ArrayList<Field>());
    }

    /**
     * Get all fields rec.
     *
     * @param clazz  the clazz
     * @param fields the fields
     * @return the field [ ]
     */
    private static Field[] getAllFieldsRec(Class clazz, List<Field> fields) {
        Class superClazz = clazz.getSuperclass();
        if (superClazz != null) {
            getAllFieldsRec(superClazz, fields);
        }

        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        return fields.toArray(new Field[fields.size()]);
    }

    /**
     * private constructor.
     */
    private Utils() {

    }

    /**
     * Remove address port.
     *
     * @param stringList the string list
     * @return the list
     */
    public static List<String> removeAddressPort(List<String> stringList) {
        List<String> adresNoPort = new ArrayList<>();

        for (String s : stringList) {
            int index = s.indexOf(":");
            if (index > -1) {
                adresNoPort.add(s.substring(0, index));
                continue;
            }
            adresNoPort.add(s);

        }
        return adresNoPort;
    }

    /**
     * Split list by comma.
     *
     * @param hosts the hosts
     * @return string
     */
    public static String splitListByComma(List<String> hosts) {
        boolean firstHost = true;
        StringBuilder hostConnection = new StringBuilder();
        for (String host : hosts) {
            if (!firstHost) {
                hostConnection.append(",");
            }
            hostConnection.append(host.trim());
            firstHost = false;
        }
        return hostConnection.toString();
    }

    /**
     * Gets extractor instance.
     *
     * @param config the config
     * @return the extractor instance
     */
    public static <T, S extends BaseConfig> IExtractor<T, S> getExtractorInstance(S config) {

        try {
            Class<T> rdd = (Class<T>) config.getExtractorImplClass();
            if (rdd == null) {
                rdd = (Class<T>) Class.forName(config.getExtractorImplClassName());
            }
            Constructor<T> c;
            if (config.getEntityClass().isAssignableFrom(Cells.class)) {
                c = rdd.getConstructor();
                return (IExtractor<T, S>) c.newInstance();
            } else {
                c = rdd.getConstructor(Class.class);
                return (IExtractor<T, S>) c.newInstance(config.getEntityClass());
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new DeepExtractorInitializationException(e.getMessage());
        }

    }

    /**
     * Cast number type.
     *
     * @param object the object
     * @param clazz  the clazz
     * @return object
     */
    public static Object castNumberType(Object object, Class clazz) {

        if (Number.class.isAssignableFrom(clazz)) {
            //            AtomicInteger, AtomicLong, BigDecimal, BigInteger, Byte, Double, Float, Integer, Long, Short
            if (Double.class.isAssignableFrom(clazz)) {
                return ((Number) object).doubleValue();
            } else if (Long.class.isAssignableFrom(clazz)) {
                return ((Number) object).longValue();

            } else if (Float.class.isAssignableFrom(clazz)) {
                return ((Number) object).floatValue();

            } else if (Integer.class.isAssignableFrom(clazz)) {
                return ((Number) object).intValue();

            } else if (Short.class.isAssignableFrom(clazz)) {
                return ((Number) object).shortValue();

            } else if (Byte.class.isAssignableFrom(clazz)) {
                return ((Number) object).byteValue();

            } else if (BigInteger.class.isAssignableFrom(clazz)) {
                return BigInteger.valueOf(((Number) object).longValue());

            } else if (BigDecimal.class.isAssignableFrom(clazz)) {
                return BigDecimal.valueOf(((Number) object).longValue());

            } else if (AtomicLong.class.isAssignableFrom(clazz)) {
                return new AtomicLong(((Number) object).longValue());

            } else if (AtomicInteger.class.isAssignableFrom(clazz)) {
                return new AtomicInteger(((Number) object).intValue());
            }
        }
        throw new ClassCastException("it is not a Number Type" + object.getClass() + "|" + clazz);
    }

    public static Object castingUtil(String value, Class classCasting) {
        Object object = value;

        //Numeric
        if (Number.class.isAssignableFrom(classCasting)) {
            if (classCasting.isAssignableFrom(Double.class)) {
                return Double.valueOf(value);
            } else if (classCasting.isAssignableFrom(Long.class)) {
                return Long.valueOf(value);

            } else if (classCasting.isAssignableFrom(Float.class)) {
                return Float.valueOf(value);

            } else if (classCasting.isAssignableFrom(Integer.class)) {
                return Integer.valueOf(value);

            } else if (classCasting.isAssignableFrom(Short.class)) {
                return Short.valueOf(value);

            } else if (classCasting.isAssignableFrom(Byte.class)) {
                return Byte.valueOf(value);
            }
        } else if (String.class.isAssignableFrom(classCasting)) {
            return object.toString();
        }
        //Class not recognise yet
        return null;

    }

    public static <S extends BaseConfig, W extends DeepJobConfig> W initConfig(S config, W deepJobConfig) {
        if (config instanceof ExtractorConfig) {
            deepJobConfig.initialize((ExtractorConfig) config);
        } else if (deepJobConfig.getClass().isAssignableFrom(config.getClass())) {
            deepJobConfig = (W) ((W) config).initialize();
        } else {
            deepJobConfig.initialize((DeepJobConfig) config);
        }
        return deepJobConfig;

    }

    /**
     * Returns an instance clone.
     * this method gets every class property by reflection, including its parents properties
     * @param t
     * @param <T>
     * @return
     */
    public static <T> T cloneObjectWithParents (T t) throws IllegalAccessException, InstantiationException {
        T clone = (T) t.getClass().newInstance();

        List<Field> allFields = new ArrayList<>();

        Class parentClass = t.getClass().getSuperclass();

        while (parentClass != null) {
            Collections.addAll(allFields, parentClass.getDeclaredFields());
            parentClass = parentClass.getSuperclass();
        }

        Collections.addAll(allFields, t.getClass().getDeclaredFields());

        for (Field field : allFields) {
            int modifiers = field.getModifiers();
            //We skip final and static fields
            if ((Modifier.FINAL & modifiers) != 0 || (Modifier.STATIC & modifiers) != 0) {
                continue;
            }
            field.setAccessible(true);

            Object value = field.get(t);

            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection collection = (Collection) field.get(clone);
                if (collection == null) {
                    collection = (Collection) field.get(t).getClass().newInstance();
                }
                collection.addAll((Collection) field.get(t));
                value = collection;
            } else if (Map.class.isAssignableFrom(field.getType())) {
                Map clonMap = (Map) field.get(t).getClass().newInstance();
                clonMap.putAll((Map) field.get(t));
                value = clonMap;
            }
            field.set(clone, value);
        }

        return clone;
    }

    /**
     * Returns an instance of ThreadPoolExecutor using an bounded queue and blocking when the worker queue is full.
     * @param nThreads thread pool size
     * @param queueSize workers queue size
     * @return thread pool executor
     */
    public static ExecutorService newBlockingFixedThreadPoolExecutor(int nThreads, int queueSize) {
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(queueSize);
        RejectedExecutionHandler blockingRejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
                try {
                    executor.getQueue().put(task);
                } catch (InterruptedException e) {
                }
            }

        };

        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS, blockingQueue,
                blockingRejectedExecutionHandler);
    }
}
