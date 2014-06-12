package com.stratio.deep.rdd.mongodb;

import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.utils.AnnotationUtils;
import com.stratio.deep.utils.Utils;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * Created by rcrespo on 12/06/14.
 */
public class UtilMongoDB {


    public static <T> T getObjectFromBson (Class<T> classEntity, BSONObject bsonObject) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = classEntity.newInstance();

        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);

        int count =fields.length;


        for (int i = 0 ; i< count ; i++){
            Method method = Utils.findSetter(fields[i].getName(), classEntity, fields[i].getType());
            method.invoke(t, bsonObject.get(AnnotationUtils.deepFieldName(fields[i])));

        }


        System.out.println(t);
        return t;
    }

    public static <T extends IDeepType> BSONObject getBsonFromObject (T t) throws IllegalAccessException, InstantiationException, InvocationTargetException {


        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        int count =fields.length;

        BSONObject bson = new BasicBSONObject();

        for (int i = 0 ; i< count ; i++){
            Method method = Utils.findGetter(fields[i].getName(), t.getClass());
            bson.put(AnnotationUtils.deepFieldName(fields[i]), method.invoke(t));
        }

        return bson;
    }


    public static void main (String[] arg) throws InstantiationException, IllegalAccessException, InvocationTargetException {
    BSONObject bson = new BasicBSONObject();
    bson.put("_text_enMongo", "prueba1");
    bson.put("_id", new ObjectId());

    getObjectFromBson(DomainEntity.class, bson);

        DomainEntity domainEntity = new DomainEntity();

        domainEntity.setId(new ObjectId());
        domainEntity.setText("prueba absoluta de orm");

        BSONObject b = getBsonFromObject(domainEntity);

        System.out.println(b);
}
}
