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

import com.stratio.deep.entity.IDeepType;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by rcrespo on 12/06/14.
 */
public class UtilMongoDB {

    /**
     * converts from BsonObject to an entity class with deep's anotations
     *
     * @param classEntity
     * @param bsonObject
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> T getObjectFromBson(Class<T> classEntity, BSONObject bsonObject) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = classEntity.newInstance();

        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);

        int count = fields.length;


        for (int i = 0; i < count; i++) {
            Method method = Utils.findSetter(fields[i].getName(), classEntity, fields[i].getType());
            method.invoke(t, bsonObject.get(AnnotationUtils.deepFieldName(fields[i])));

        }


        return t;
    }

    /**
     * converts from an entity class with deep's anotations to BsonObject
     *
     * @param t
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T extends IDeepType> BSONObject getBsonFromObject(T t) throws IllegalAccessException, InstantiationException, InvocationTargetException {


        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        int count = fields.length;

        BSONObject bson = new BasicBSONObject();

        for (int i = 0; i < count; i++) {
            Method method = Utils.findGetter(fields[i].getName(), t.getClass());
            bson.put(AnnotationUtils.deepFieldName(fields[i]), method.invoke(t));
        }

        return bson;
    }

    /**
     * returns the id value annotated with @DeepField(fieldName = "_id")
     *
     * @param t
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T extends IDeepType> Object getId(T t) throws IllegalAccessException, InstantiationException, InvocationTargetException {


        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());


        int count = fields.length;


        for (int i = 0; i < count; i++) {
            if (AnnotationUtils.deepFieldName(fields[i]).equals("_id")) {
                return Utils.findGetter(fields[i].getName(), t.getClass()).invoke(t);
            }

        }

        return null;
    }

}
