/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.mongodb.extractor;

import java.lang.reflect.InvocationTargetException;

import com.mongodb.DBObject;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 7/11/14.
 *
 * @param <T>  the type parameter
 */
public final class MongoNativeEntityExtractor<T> extends MongoNativeExtractor<T, MongoDeepJobConfig<T>> {
    /**
     * The constant serialVersionUID.
     */
    private static final long serialVersionUID = -1073974965338697939L;

    /**
     * Instantiates a new Mongo native entity extractor.
     *
     * @param t the t
     */
    public MongoNativeEntityExtractor(Class<T> t){
        this.mongoDeepJobConfig = new MongoDeepJobConfig<>(t);
    }


    @Override
    protected T transformElement(DBObject dbObject) {
        try {
            return (T) UtilMongoDB.getObjectFromBson(mongoDeepJobConfig.getEntityClass(), dbObject);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected DBObject transformElement(T entity) {
        try {
            return (DBObject) UtilMongoDB.getBsonFromObject(entity);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

}
