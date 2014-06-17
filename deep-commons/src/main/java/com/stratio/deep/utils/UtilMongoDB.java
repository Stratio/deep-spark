package com.stratio.deep.utils;

import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.utils.AnnotationUtils;
import com.stratio.deep.utils.Utils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by rcrespo on 12/06/14.
 */
public class UtilMongoDB {


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


    public static <T extends IDeepType> Object getId(T t) throws IllegalAccessException, InstantiationException, InvocationTargetException {


        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());


        int count = fields.length;


        for (int i = 0; i < count; i++) {
            if (AnnotationUtils.deepFieldName(fields[i]).equals("_id")) {
                System.out.println("encuentro el valor del campo " + fields[i].getName());
                return Utils.findGetter(fields[i].getName(), t.getClass()).invoke(t);
            }

        }

        return null;
    }

}
