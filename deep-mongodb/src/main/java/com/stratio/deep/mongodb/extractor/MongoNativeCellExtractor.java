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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.Partition;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.commons.utils.Pair;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.partition.MongoPartition;
import com.stratio.deep.mongodb.reader.MongoReader;
import com.stratio.deep.mongodb.utils.UtilMongoDB;
import com.stratio.deep.mongodb.writer.MongoWriter;

/**
 * Created by rcrespo on 29/10/14.
 * @param <S>  the type parameter
 */
public class MongoNativeCellExtractor<S extends BaseConfig<Cells>> extends MongoNativeExtractor<Cells,S> {

    /**
     * The constant serialVersionUID.
     */
    private static final long serialVersionUID = 7584233937780968953L;

    /**
     * Instantiates a new Mongo native cell extractor.
     */
    public MongoNativeCellExtractor(){
        this.mongoDeepJobConfig = new MongoDeepJobConfig<>(Cells.class);
    }

    @Override
    protected Cells transformElement(DBObject dbObject) {
        try {
            return UtilMongoDB.getCellFromBson(dbObject,  mongoDeepJobConfig.getNameSpace());
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
