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
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 29/10/14.
 */
public class MongoNativeCellExtractor extends MongoNativeExtractor<Cells, MongoDeepJobConfig<Cells>> {

    /**
     * The constant serialVersionUID.
     */
    private static final long serialVersionUID = 7584233937780968953L;

    /**
     * Instantiates a new Mongo native cell extractor.
     */
    public MongoNativeCellExtractor() {
        this.mongoDeepJobConfig = new MongoDeepJobConfig<>(Cells.class);
    }

    @Override
    protected Cells transformElement(DBObject dbObject) {
        try {
            return UtilMongoDB.getCellFromBson(dbObject, mongoDeepJobConfig.getNameSpace());
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected DBObject transformElement(Cells entity) {
        try {
            return UtilMongoDB.getDBObjectFromCell(entity);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
}
