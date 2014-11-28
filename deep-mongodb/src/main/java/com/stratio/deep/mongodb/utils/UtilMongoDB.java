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

package com.stratio.deep.mongodb.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.utils.AnnotationUtils;
import com.stratio.deep.commons.utils.Utils;

/**
 * Several utilities to work used in the Spark <=> MongoDB integration.
 */
public final class UtilMongoDB {

    /**
     * The constant MONGO_DEFAULT_ID.
     */
    public static final String MONGO_DEFAULT_ID = "_id";

    /**
     * The constant LOG.
     */
    private static final Logger LOG = LoggerFactory.getLogger(UtilMongoDB.class);

    /**
     * Private default constructor.
     */

    private UtilMongoDB() {
        throw new UnsupportedOperationException();
    }

    /**
     * converts from BsonObject to an entity class with deep's anotations
     *
     * @param classEntity the entity name.
     * @param bsonObject  the instance of the BSONObjet to convert.
     * @return the provided bsonObject converted to an instance of T.
     * @throws IllegalAccessException the illegal access exception
     * @throws IllegalAccessException the instantiation exception
     * @throws IllegalAccessException the invocation target exception
     */
    public static <T> T getObjectFromBson(Class<T> classEntity, BSONObject bsonObject)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = classEntity.newInstance();

        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);

        Object insert = null;

        for (Field field : fields) {
            Object currentBson = null;
            Method method = null;
            try {
                method = Utils.findSetter(field.getName(), classEntity, field.getType());

                Class<?> classField = field.getType();

                currentBson = bsonObject.get(AnnotationUtils.deepFieldName(field));
                if (currentBson != null) {

                    if (Iterable.class.isAssignableFrom(classField)) {
                        Type type = field.getGenericType();

                        insert = subDocumentListCase(type, (List) bsonObject.get(AnnotationUtils.deepFieldName(field)));

                    } else if (IDeepType.class.isAssignableFrom(classField)) {
                        insert = getObjectFromBson(classField, (BSONObject) bsonObject.get(AnnotationUtils.deepFieldName
                                (field)));
                    } else {
                        insert = currentBson;
                    }
                    method.invoke(t, insert);
                }
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException | IllegalArgumentException e) {
                LOG.error("impossible to create a java object from Bson field:" + field.getName() + " and type:" + field
                        .getType() + " and value:" + t + "; bsonReceived:" + currentBson + ", bsonClassReceived:"
                        + currentBson.getClass());

                method.invoke(t, Utils.castNumberType(insert, t));
            }

        }

        return t;
    }

    /**
     * Sub document list case.
     *
     * @param <T>       the type parameter
     * @param type      the type
     * @param bsonOject the bson oject
     * @return the object
     * @throws IllegalAccessException    the illegal access exception
     * @throws InstantiationException    the instantiation exception
     * @throws InvocationTargetException the invocation target exception
     */
    private static <T> Object subDocumentListCase(Type type, List<T> bsonOject)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        ParameterizedType listType = (ParameterizedType) type;

        Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];

        List list = new ArrayList();
        for (T t : bsonOject) {
            list.add(getObjectFromBson(listClass, (BSONObject) t));
        }

        return list;
    }

    /**
     * converts from an entity class with deep's anotations to BsonObject.
     *
     * @param t an instance of an object of type T to convert to BSONObject.
     * @return the provided object converted to BSONObject.
     * @throws IllegalAccessException the illegal access exception
     * @throws IllegalAccessException the instantiation exception
     * @throws IllegalAccessException the invocation target exception
     */
    public static <T> DBObject getBsonFromObject(T t)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        DBObject bson = new BasicDBObject();

        for (Field field : fields) {
            Method method = Utils.findGetter(field.getName(), t.getClass());
            Object object = method.invoke(t);
            if (object != null) {
                if (Collection.class.isAssignableFrom(field.getType())) {
                    Collection c = (Collection) object;
                    Iterator iterator = c.iterator();
                    List<DBObject> innerBsonList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        innerBsonList.add(getBsonFromObject((IDeepType) iterator.next()));
                    }
                    bson.put(AnnotationUtils.deepFieldName(field), innerBsonList);
                } else if (IDeepType.class.isAssignableFrom(field.getType())) {
                    bson.put(AnnotationUtils.deepFieldName(field), getBsonFromObject((IDeepType) object));
                } else {
                    bson.put(AnnotationUtils.deepFieldName(field), object);
                }
            }
        }

        return bson;
    }

    /**
     * returns the id value annotated with @DeepField(fieldName = "_id")
     *
     * @param t an instance of an object of type T to convert to BSONObject.
     * @return the provided object converted to Object.
     * @throws IllegalAccessException the illegal access exception
     * @throws IllegalAccessException the instantiation exception
     * @throws IllegalAccessException the invocation target exception
     */
    public static <T extends IDeepType> Object getId(T t)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        for (Field field : fields) {
            if (MONGO_DEFAULT_ID.equals(AnnotationUtils.deepFieldName(field))) {
                return Utils.findGetter(field.getName(), t.getClass()).invoke(t);
            }

        }

        return null;
    }

    /**
     * converts from BsonObject to cell class with deep's anotations
     *
     * @param bsonObject the bson object
     * @param tableName  the table name
     * @return cell from bson
     * @throws IllegalAccessException the illegal access exception
     * @throws IllegalAccessException the instantiation exception
     * @throws IllegalAccessException the invocation target exception
     */
    public static Cells getCellFromBson(BSONObject bsonObject, String tableName) {

        Cells cells = tableName != null ? new Cells(tableName) : new Cells();

        Map<String, Object> map = bsonObject.toMap();

        Set<Map.Entry<String, Object>> entryBson = map.entrySet();

        for (Map.Entry<String, Object> entry : entryBson) {
            try {

                if (List.class.isAssignableFrom(entry.getValue().getClass())) {
                    List<Cells> innerCell = new ArrayList<>();
                    for (BSONObject innerBson : (List<BSONObject>) entry.getValue()) {
                        innerCell.add(getCellFromBson(innerBson, null));
                    }
                    cells.add(Cell.create(entry.getKey(), innerCell));
                } else if (BSONObject.class.isAssignableFrom(entry.getValue().getClass())) {
                    Cells innerCells = getCellFromBson((BSONObject) entry.getValue(), null);
                    cells.add(Cell.create(entry.getKey(), innerCells));
                } else {
                    cells.add(Cell.create(entry.getKey(), entry.getValue()));
                }
            } catch (IllegalArgumentException e) {
                LOG.error("impossible to create a java cell from Bson field:" + entry.getKey() + ", type:" + entry
                        .getValue().getClass() + ", value:" + entry.getValue());
            }

        }
        return cells;
    }

    /**
     * converts from and entity class with deep's anotations to BsonObject
     *
     * @param cells the cells
     * @return bson from cell
     */
    public static BSONObject getBsonFromCell(Cells cells) {

        BSONObject bson = new BasicBSONObject();
        for (Cell cell : cells) {
            if (cell.getValue() != null) {
                if (Collection.class.isAssignableFrom(cell.getCellValue().getClass())) {
                    Collection c = (Collection) cell.getCellValue();
                    Iterator iterator = c.iterator();
                    List<BSONObject> innerBsonList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        innerBsonList.add(getBsonFromCell((Cells) iterator.next()));
                    }
                    bson.put(cell.getCellName(), innerBsonList);
                } else if (Cells.class.isAssignableFrom(cell.getCellValue().getClass())) {
                    bson.put(cell.getCellName(), getBsonFromCell((Cells) cell.getCellValue()));
                } else {
                    bson.put(cell.getCellName(), cell.getCellValue());
                }
            }

        }

        return bson;
    }

    /**
     * converts from and entity class with deep's anotations to BsonObject
     *
     * @param cells the cells
     * @return bson from cell
     */
    public static DBObject getDBObjectFromCell(Cells cells) {

        DBObject bson = new BasicDBObject();
        for (Cell cell : cells) {
            if (cell.getValue() != null) {
                if (Collection.class.isAssignableFrom(cell.getCellValue().getClass())) {
                    Collection c = (Collection) cell.getCellValue();
                    Iterator iterator = c.iterator();
                    List<DBObject> innerBsonList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        innerBsonList.add(getDBObjectFromCell((Cells) iterator.next()));
                    }
                    bson.put(cell.getCellName(), innerBsonList);
                } else if (Cells.class.isAssignableFrom(cell.getCellValue().getClass())) {
                    bson.put(cell.getCellName(), getBsonFromCell((Cells) cell.getCellValue()));
                } else {
                    bson.put(cell.getCellName(), cell.getCellValue());
                }
            }
        }

        return bson;
    }

}
