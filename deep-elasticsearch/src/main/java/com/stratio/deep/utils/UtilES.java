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

/**
 * Several utilities to work used in the Spark <=> ElasticSearch integration.
 */

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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.json.simple.JSONObject;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterOperator;
import com.stratio.deep.commons.utils.AnnotationUtils;
import com.stratio.deep.commons.utils.Utils;
import com.stratio.deep.commons.entity.IDeepType;
import org.apache.hadoop.io.*;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.*;

/**
 * Created by rcrespo on 29/07/14.
 */
public final class UtilES {

    private static final Logger LOG = LoggerFactory.getLogger(UtilES.class);

    /**
     * Private default constructor.
     */
    private UtilES() {
        throw new UnsupportedOperationException();
    }

    /**
     * converts from JSONObject to an entity class with deep's anotations
     *
     * @param classEntity the entity name.
     * @param jsonObject  the instance of the JSONObject to convert.
     * @param <T>         return type.
     * @return the provided JSONObject converted to an instance of T.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws java.lang.reflect.InvocationTargetException
     */
    public static <T> T getObjectFromJson(Class<T> classEntity, LinkedMapWritable jsonObject)
            throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        T t = classEntity.newInstance();

        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);

        Object insert;

        for (Field field : fields) {
            Method method = Utils.findSetter(field.getName(), classEntity, field.getType());

            Class<?> classField = field.getType();
            String key = AnnotationUtils.deepFieldName(field);
            Text text = new org.apache.hadoop.io.Text(key);
            Writable currentJson = jsonObject.get(text);
            if (currentJson != null) {

                if (Iterable.class.isAssignableFrom(classField)) {
                    Type type = field.getGenericType();
                    insert = subDocumentListCase(type, (ArrayWritable) currentJson);
                    method.invoke(t, (insert));

                } else if (IDeepType.class.isAssignableFrom(classField)) {
                    insert = getObjectFromJson(classField, (LinkedMapWritable) currentJson);
                    method.invoke(t, (insert));
                } else {
                    insert = currentJson;
                    try{
                        method.invoke(t, getObjectFromWritable((Writable) insert));
                    }
                    catch(Exception e){
                        LOG.error("impossible to convert field " +t + " :"+ field + " error: "+ e.getMessage());
                        method.invoke(t, Utils.castNumberType(getObjectFromWritable((Writable) insert), t));
                    }

                }

            }
        }

        return t;
    }






    private static <T> Object subDocumentListCase(Type type, ArrayWritable arrayWritable) throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        ParameterizedType listType = (ParameterizedType) type;

        Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];

        List list = new ArrayList();
        Writable[] writetable = arrayWritable.get();

        for (int i = 0; i < writetable.length; i++) {
            list.add(getObjectFromJson(listClass, (LinkedMapWritable) writetable[i]));
        }

        return list;
    }

    /**
     * converts from an entity class with deep's anotations to JSONObject.
     *
     * @param t   an instance of an object of type T to convert to JSONObject.
     * @param <T> the type of the object to convert.
     * @return the provided object converted to JSONObject.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> JSONObject getJsonFromObject(T t)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        JSONObject json = new JSONObject();

        for (Field field : fields) {
            Method method = Utils.findGetter(field.getName(), t.getClass());
            Object object = method.invoke(t);
            if (object != null) {
                if (Collection.class.isAssignableFrom(field.getType())) {
                    Collection c = (Collection) object;
                    Iterator iterator = c.iterator();
                    List<JSONObject> innerJsonList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        innerJsonList.add(getJsonFromObject((IDeepType) iterator.next()));
                    }
                    json.put(AnnotationUtils.deepFieldName(field), innerJsonList);
                } else if (IDeepType.class.isAssignableFrom(field.getType())) {
                    json.put(AnnotationUtils.deepFieldName(field), getJsonFromObject((IDeepType) object));
                } else {
                    json.put(AnnotationUtils.deepFieldName(field), object);
                }
            }
        }

        return json;
    }

    /**
     * converts from an entity class with deep's anotations to JSONObject.
     *
     * @param t   an instance of an object of type T to convert to JSONObject.
     * @param <T> the type of the object to convert.
     * @return the provided object converted to JSONObject.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> LinkedMapWritable getLinkedMapWritableFromObject(T t)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        LinkedMapWritable linkedMapWritable = new LinkedMapWritable();

        for (Field field : fields) {
            Method method = Utils.findGetter(field.getName(), t.getClass());
            Object object = method.invoke(t);
            if (object != null) {
                if (Collection.class.isAssignableFrom(field.getType())) {
                    Collection c = (Collection) object;
                    Iterator iterator = c.iterator();
                    List<LinkedMapWritable> innerJsonList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        innerJsonList.add(getLinkedMapWritableFromObject((IDeepType) iterator.next()));
                    }
                    //linkedMapWritable.put(new Text(AnnotationUtils.deepFieldName(field)), new LinkedMapWritable[innerJsonList.size()]);
                } else if (IDeepType.class.isAssignableFrom(field.getType())) {
                    linkedMapWritable.put(new Text(AnnotationUtils.deepFieldName(field)),
                            getLinkedMapWritableFromObject((IDeepType) object));
                } else {
                    linkedMapWritable
                            .put(new Text(AnnotationUtils.deepFieldName(field)), getWritableFromObject(object));
                }
            }
        }

        return linkedMapWritable;
    }

    /**
     * returns the id value annotated with @DeepField(fieldName = "_id")
     *
     * @param t   an instance of an object of type T to convert to JSONObject.
     * @param <T> the type of the object to convert.
     * @return the provided object converted to Object.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T extends IDeepType> Object getId(T t)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        //TODO : implement

        return null;
    }

    /**
     * converts from JSONObject to cell class
     *
     * @param jsonObject
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static Cells getCellFromJson(LinkedMapWritable jsonObject, String tableName) throws IllegalAccessException,
            InstantiationException, InvocationTargetException, NoSuchMethodException {

        Cells cells = tableName!= null ?new Cells(tableName): new Cells();

        Set<Map.Entry<Writable, Writable>> entryJson = jsonObject.entrySet();

        for (Map.Entry<Writable, Writable> entry : entryJson) {

            if (LinkedMapWritable.class.isAssignableFrom(entry.getValue().getClass())) {
                Cells innerCells = getCellFromJson((LinkedMapWritable) entry.getValue(), tableName);
                cells.add(Cell.create(entry.getKey().toString(), innerCells));
            } else if (ArrayWritable.class.isAssignableFrom(entry.getValue().getClass())) {
                Writable[] writetable = ((ArrayWritable) entry.getValue()).get();
                List<Cells> innerCell = new ArrayList<>();
                for (int i = 0 ; i < writetable.length ; i++){
                    innerCell.add(getCellFromJson((LinkedMapWritable) writetable[i], tableName));
                }
                cells.add(Cell.create(entry.getKey().toString(), innerCell));
            } else {

                cells.add(Cell.create(entry.getKey().toString(), getObjectFromWritable(entry.getValue())));
            }

        }
        return cells;
    }

    /**
     * Returns the object inside Writable
     *
     * @param writable
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    private static Object getObjectFromWritable(Writable writable)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Object object = null;

        if (writable instanceof NullWritable) {
            object = NullWritable.get();
        } else if (writable instanceof BooleanWritable) {
            object = ((BooleanWritable) writable).get();
        } else if (writable instanceof Text) {
            object = writable.toString();
        } else if (writable instanceof ByteWritable) {
            object = ((ByteWritable) writable).get();
        } else if (writable instanceof IntWritable) {
            object = ((IntWritable) writable).get();
        } else if (writable instanceof LongWritable) {
            object = ((LongWritable) writable).get();
        } else if (writable instanceof BytesWritable) {
            object = ((BytesWritable) writable).getBytes();
        } else if (writable instanceof DoubleWritable) {
            object = ((DoubleWritable) writable).get();
        } else if (writable instanceof FloatWritable) {
            object = ((FloatWritable) writable).get();
        } else {
            //TODO : do nothing
        }

        return object;
    }

    /**
     * Returns the object inside Writable
     *
     * @param object
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    private static Writable getWritableFromObject(Object object) {

        Writable writable = null;

        if (object instanceof String) {

            writable = new Text(object.toString());

        } else if (object instanceof Long) {

            writable = new LongWritable((Long) object);

        } else {

            writable = new IntWritable((Integer) object);

        }
        // writable = writable!=null?writable:new Text("");
        return writable;
    }

    /**
     * converts from cell class to JSONObject
     *
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static JSONObject getJsonFromCell(Cells cells)
            throws IllegalAccessException, InstantiationException, InvocationTargetException {

        JSONObject json = new JSONObject();
        for (Cell cell : cells) {
            if (cell.getCellValue() != null) {
                if (Collection.class.isAssignableFrom(cell.getCellValue().getClass())) {
                    Collection c = (Collection) cell.getCellValue();
                    Iterator iterator = c.iterator();
                    List<JSONObject> innerJsonList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        innerJsonList.add(getJsonFromCell((Cells) iterator.next()));
                    }
                    json.put(cell.getCellName(), innerJsonList);
                } else if (Cells.class.isAssignableFrom(cell.getCellValue().getClass())) {
                    json.put(cell.getCellName(), getJsonFromCell((Cells) cell.getCellValue()));
                } else {
                    json.put(cell.getCellName(), cell.getCellValue());
                }

            }
        }

        return json;
    }


    public static QueryBuilder generateQuery (Filter... filterArray){
        List<Filter> range = new ArrayList<>();
        List<Filter> ne = new ArrayList<>();
        List<Filter> is = new ArrayList<>();
        BoolQueryBuilder qb = QueryBuilders.boolQuery();


        for (Filter filter : filterArray){
            if (! filter.getOperation().equals(FilterOperator.IS) && !filter.getOperation().equals(FilterOperator.NE)){
                range.add(filter);
            }else if (filter.getOperation().equals(FilterOperator.IS)) {
                is.add(filter);
            }else{
                ne.add(filter);
            }
        }


        //        List<RangeQueryBuilder> rangeQueryBuilders = new ArrayList<>();
        for (Filter filter : range){

            RangeQueryBuilder rangeQueryBuilder = QueryBuilders
                    .rangeQuery(filter.getField());
            switch (filter.getOperation()){
            case FilterOperator.LT:
                rangeQueryBuilder.lt(filter.getValue());
                break;
            case FilterOperator.LTE:
                rangeQueryBuilder.lte(filter.getValue());
                break;
            case FilterOperator.GT:
                rangeQueryBuilder.gt(filter.getValue());
                break;
            case FilterOperator.GTE:
                rangeQueryBuilder.gte(filter.getValue());
                break;
            default:
                break;
            }
            qb.must(rangeQueryBuilder);
            //            rangeQueryBuilders.add(rangeQueryBuilder);
        }

        //        for(RangeQueryBuilder rangeQueryBuilder : rangeQueryBuilders){
        //            qb.must(rangeQueryBuilder);
        //        }

        for(Filter filter : is){
            qb.must(QueryBuilders.matchQuery(filter.getField(), filter.getValue()));
        }

        for(Filter filter : ne){
            qb.mustNot(QueryBuilders.matchQuery(filter.getField(), filter.getValue()));
        }
//        System.out.println("imprimo el query builder resultante :) " +qb.toString());
        return qb;
    }




}
