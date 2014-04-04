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

import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
     * The first Cells element contains the list of Cell elements that represent the key (partition + cluster key). <br/>
     * The second Cells element contains all the other columns.
     *
     * @param cells
     * @return
     */
    public static Tuple2<Cells, Cells> cellList2tuple(Cells cells) {
        Cells keys = new Cells();
        Cells values = new Cells();

        for (Cell<?> c : cells) {

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
     * @param e
     * @param <T>
     * @return
     */
    public static <T extends IDeepType> Tuple2<Cells, Cells> deepType2tuple(T e) {

        Pair<Field[], Field[]> fields = AnnotationUtils.filterKeyFields(e.getClass().getDeclaredFields());

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
     * @param additionalFilters
     * @return
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
                value = cleanFilterString(entry, value.trim());
            }

            sb.append(" AND ").append(quote(entry.getKey())).append(" = ").append(value);
        }

        return sb.toString();
    }

    private static String cleanFilterString(Map.Entry<String, Serializable> entry, String value) {
        if (value.contains("\"")) {
            throw new DeepGenericException("value for filter \'" + entry.getKey() + "\' contains double quotes, please check your syntax.");
        }

        return singleQuote(value);
    }

    /**
     * Generates a create table cql statement from the given Cells description.
     *
     * @param keys
     * @param values
     * @param outputKeyspace
     * @param outputColumnFamily
     * @return
     */
    public static String createTableQueryGenerator(Cells keys, Cells values, String outputKeyspace,
        String outputColumnFamily) {

        if (keys == null || StringUtils.isEmpty(outputKeyspace)
            || StringUtils.isEmpty(outputColumnFamily)) {
            throw new DeepGenericException("keys, outputKeyspace and outputColumnFamily cannot be null");
        }

        StringBuffer sb = new StringBuffer("CREATE TABLE ").append(outputKeyspace)
            .append(".").append(outputColumnFamily).append(" (");

        List<String> partitionKey = new ArrayList();
        List<String> clusterKey = new ArrayList();

        boolean isFirstField = true;

        for (Cell<?> key : keys) {
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
            for (Cell<?> key : values) {
                sb.append(", ");
                sb.append(quote(key.getCellName())).append(" ").append(key.marshaller().asCQL3Type().toString());
            }
        }

        StringBuffer partitionKeyToken = new StringBuffer("(");

        isFirstField = true;
        for (String s : partitionKey) {
            if (!isFirstField) {
                partitionKeyToken.append(", ");
            }
            partitionKeyToken.append(s);
            isFirstField = false;
        }

        partitionKeyToken.append(")");

        StringBuffer clusterKeyToken = new StringBuffer("");

        isFirstField = true;
        for (String s : clusterKey) {
            if (!isFirstField) {
                clusterKeyToken.append(", ");
            }
            clusterKeyToken.append(s);
            isFirstField = false;
        }

        StringBuffer keyPart = new StringBuffer(", PRIMARY KEY ");

        if (clusterKey.size() > 0) {
            keyPart.append("(");
        }

        keyPart.append(partitionKeyToken);

        if (clusterKey.size() > 0) {
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
     * @param outputKeyspace
     * @param outputColumnFamily
     * @return
     */
    public static String updateQueryGenerator(Cells keys, Cells values, String outputKeyspace,
        String outputColumnFamily) {

        StringBuilder sb = new StringBuilder("UPDATE ").append(outputKeyspace).append(".").append(outputColumnFamily)
            .append(" SET ");

        int k = 0;

        StringBuilder keyClause = new StringBuilder(" WHERE ");
        for (Cell<?> cell : keys.getCells()) {
            if (cell.isPartitionKey() || cell.isClusterKey()) {
                if (k > 0) {
                    keyClause.append(" AND ");
                }

                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

                ++k;
            }

        }

        k = 0;
        for (Cell<?> cell : values.getCells()) {
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
     * @param statements
     * @return
     */
    public static String batchQueryGenerator(List<String> statements) {
        StringBuffer sb = new StringBuffer("BEGIN BATCH \n");

        for (int i = 0; i < statements.size(); i++) {
            String statement = statements.get(i);
            sb.append(statement).append("\n");
        }

        sb.append(" APPLY BATCH;");

        return sb.toString();
    }

    /**
     * Splits columns names and values as required by Datastax java driver to generate an Insert query.
     *
     * @param tuple
     * @return
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

        return new Tuple2<String[], Object[]>(names, values);
    }

    /**
     * Resolves the setter name for the property whose name is 'propertyName' whose type is 'valueType'
     * in the entity bean whose class is 'entityClass'.
     * If we don't find a setter following Java's naming conventions, before throwing an exception we try to
     * resolve the setter following Scala's naming conventions.
     *
     * @param propertyName the field name of the property whose setter we want to resolve.
     * @param entityClass the bean class object in which we want to search for the setter.
     * @param valueType the class type of the object that we want to pass to the setter.
     * @return
     */
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
     * Returns an instance of the Cassandra validator that matches the provided object.
     *
     * @param obj
     * @param <T>
     * @return an instance of the Cassandra validator that matches the provided object.
     * @throws com.stratio.deep.exception.DeepGenericException if no validator can be found for the specified object.
     */
    public static <T> AbstractType<?> marshallerInstance(T obj){
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
            throw new DeepGenericException("parameter class " + obj.getClass().getCanonicalName() + " does not have a Cassandra marshaller");
        }

        return abstractType;
    }

    /**
     * private constructor.
     */
    private Utils() {

    }

}
