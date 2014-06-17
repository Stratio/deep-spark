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

package com.stratio.deep.utils;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import com.stratio.deep.utils.Pair;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.stratio.deep.utils.AnnotationUtils.MAP_JAVA_TYPE_TO_ABSTRACT_TYPE;

/**
 * Utility class providing useful methods to manipulate the conversion
 * between ByteBuffers maps coming from the underlying Cassandra API to
 * instances of a concrete javabean.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class Utils {

    /**
     * Utility method that converts an IDeepType to tuple of two Cells.<br/>
     * The first Cells element contains the list of Cell elements that represent the key (partition + cluster key).
     * <br/>
     * The second Cells element contains all the other columns.
     *
     * @param cells the Cells object to process.
     * @return a pair whose first element is a Cells object containing key Cell(s) and whose second element contains all of the other Cell(s).
     */
    public static Tuple2<Cells, Cells> cellList2tuple(Cells cells) {
        Cells keys = new Cells();
        Cells values = new Cells();

        for (Cell c : cells) {

            if (c.isPartitionKey() || c.isClusterKey()) {

                keys.add(c);
            } else {
                values.add(c);
            }
        }

        return new Tuple2<>(keys, values);
    }

    /**
     * Convers an instance of type <T> to a tuple of ( Map<String, ByteBuffer>, List<ByteBuffer> ).
     * The first map contains the key column names and the corresponding values.
     * The ByteBuffer list contains the value of the columns that will be bounded to CQL query parameters.
     *
     * @param e the entity object to process.
     * @param <T> the entity object generic type.
     * @return a pair whose first element is a Cells object containing key Cell(s) and whose second element contains all of the other Cell(s).
     */
    public static <T extends IDeepType> Tuple2<Cells, Cells> deepType2tuple(T e) {

        Pair<Field[], Field[]> fields = AnnotationUtils.filterKeyFields(e.getClass());

        Field[] keyFields = fields.left;
        Field[] otherFields = fields.right;

        Cells keys = new Cells();
        Cells values = new Cells();

        for (Field keyField : keyFields) {
            keys.add(Cell.create(e, keyField));
        }

        for (Field valueField : otherFields) {
            values.add(Cell.create(e, valueField));
        }

        return new Tuple2<>(keys, values);
    }

    /**
     * Creates a new instance of the given class.
     *
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
     * @param className the class object for which a new instance should be created.
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
     * Generates the part of the query where clause that will hit the Cassandra's secondary indexes.
     *
     * @param additionalFilters the map of filters names and values.
     * @return the query subpart corresponding to the provided additional filters.
     */
    public static String additionalFilterGenerator(Map<String, Serializable> additionalFilters) {
        if (MapUtils.isEmpty(additionalFilters)) {
            return "";
        }

        StringBuilder sb = new StringBuilder("");

        for (Map.Entry<String, Serializable> entry : additionalFilters.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            String value = entry.getValue().toString();

            if (entry.getValue() instanceof String) {
                value = singleQuote(value.trim());
            }

            sb.append(" AND ").append(quote(entry.getKey())).append(" = ").append(value);
        }

        return sb.toString();
    }

    /**
     * Generates a create table cql statement from the given Cells description.
     *
     * @param keys the row  keys wrapped inside a Cells object.
     * @param values all the other row columns wrapped inside a Cells object.
     * @param outputKeyspace the output keyspace.
     * @param outputColumnFamily the output column family.
     * @return the create table statement.
     */
    public static String createTableQueryGenerator(Cells keys, Cells values, String outputKeyspace,
                                                   String outputColumnFamily) {

        if (keys == null || StringUtils.isEmpty(outputKeyspace)
                || StringUtils.isEmpty(outputColumnFamily)) {
            throw new DeepGenericException("keys, outputKeyspace and outputColumnFamily cannot be null");
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(outputKeyspace)
                .append(".").append(outputColumnFamily).append(" (");

        List<String> partitionKey = new ArrayList<>();
        List<String> clusterKey = new ArrayList<>();

        boolean isFirstField = true;

        for (Cell key : keys) {
            String cellName = quote(key.getCellName());

            if (!isFirstField) {
                sb.append(", ");
            }

            sb.append(cellName).append(" ").append(key.marshaller().asCQL3Type().toString());

            if (key.isPartitionKey()) {
                partitionKey.add(cellName);
            } else if (key.isClusterKey()) {
                clusterKey.add(cellName);
            }

            isFirstField = false;
        }

        if (values != null) {
            for (Cell key : values) {
                sb.append(", ");
                sb.append(quote(key.getCellName())).append(" ").append(key.marshaller().asCQL3Type().toString());
            }
        }

        StringBuilder partitionKeyToken = new StringBuilder("(");

        isFirstField = true;
        for (String s : partitionKey) {
            if (!isFirstField) {
                partitionKeyToken.append(", ");
            }
            partitionKeyToken.append(s);
            isFirstField = false;
        }

        partitionKeyToken.append(")");

        StringBuilder clusterKeyToken = new StringBuilder("");

        isFirstField = true;
        for (String s : clusterKey) {
            if (!isFirstField) {
                clusterKeyToken.append(", ");
            }
            clusterKeyToken.append(s);
            isFirstField = false;
        }

        StringBuilder keyPart = new StringBuilder(", PRIMARY KEY ");

        if (!clusterKey.isEmpty()) {
            keyPart.append("(");
        }

        keyPart.append(partitionKeyToken);

        if (!clusterKey.isEmpty()) {
            keyPart.append(", ");
            keyPart.append(clusterKeyToken);
            keyPart.append(")");
        }

        sb.append(keyPart).append(");");

        return sb.toString();
    }

    /**
     * Generates the update query for the provided IDeepType.
     * The UPDATE query takes into account all the columns of the testentity, even those containing the null value.
     * We do not generate the key part of the update query. The provided query will be concatenated with the key part
     * by CqlRecordWriter.
     *
     * @param keys the row  keys wrapped inside a Cells object.
     * @param values all the other row columns wrapped inside a Cells object.
     * @param outputKeyspace the output keyspace.
     * @param outputColumnFamily the output column family.
     * @return the update query statement.
     */
    public static String updateQueryGenerator(Cells keys, Cells values, String outputKeyspace,
                                              String outputColumnFamily) {

        StringBuilder sb = new StringBuilder("UPDATE ").append(outputKeyspace).append(".").append(outputColumnFamily)
                .append(" SET ");

        int k = 0;

        StringBuilder keyClause = new StringBuilder(" WHERE ");
        for (Cell cell : keys.getCells()) {
            if (cell.isPartitionKey() || cell.isClusterKey()) {
                if (k > 0) {
                    keyClause.append(" AND ");
                }

                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

                ++k;
            }

        }

        k = 0;
        for (Cell cell : values.getCells()) {
            if (k > 0) {
                sb.append(", ");
            }

            sb.append(String.format("%s = ?", quote(cell.getCellName())));
            ++k;
        }

        sb.append(keyClause).append(";");

        return sb.toString();
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
     * Returns an instance of the Cassandra validator that matches the provided object.
     *
     * @param obj the object to use to resolve the cassandra marshaller.
     * @param <T> the generic object type.
     * @return an instance of the Cassandra validator that matches the provided object.
     * @throws com.stratio.deep.exception.DeepGenericException if no validator can be found for the specified object.
     */
    public static <T> AbstractType<?> marshallerInstance(T obj) {
        AbstractType<?> abstractType = MAP_JAVA_TYPE_TO_ABSTRACT_TYPE.get(obj.getClass());

        if (obj instanceof UUID) {
            UUID uuid = (UUID) obj;

            if (uuid.version() == 1) {
                abstractType = TimeUUIDType.instance;

            } else {
                abstractType = UUIDType.instance;
            }
        }

        if (abstractType == null) {
            throw new DeepGenericException("parameter class " + obj.getClass().getCanonicalName() + " does not have a" +
                    " Cassandra marshaller");
        }

        return abstractType;
    }

    /**
     * Returns the inet address for the specified location.
     *
     * @param location the address as String
     * @return the InetAddress object associated to the provided address.
     */
    public static InetAddress inetAddressFromLocation(String location) {
        try {
            return InetAddress.getByName(location);
        } catch (UnknownHostException e) {
            throw new DeepIOException(e);
        }
    }

    /**
     * Return the set of fields declared at all level of class hierachy
     */
    public static Field[] getAllFields(Class clazz) {
        return getAllFieldsRec(clazz, new ArrayList<Field>());
    }

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

}
