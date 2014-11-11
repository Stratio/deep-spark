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
import java.util.List;

import org.apache.spark.Partition;

import com.mongodb.DBObject;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 7/11/14.
 */
public class MongoNativeEntityExtractor<T, S extends BaseConfig<T>> extends MongoNativeExtractor<T, S> {
    private static final long serialVersionUID = -1073974965338697939L;

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
