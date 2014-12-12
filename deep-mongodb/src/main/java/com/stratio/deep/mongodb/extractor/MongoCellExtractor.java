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

package com.stratio.deep.mongodb.extractor;

import java.lang.reflect.InvocationTargetException;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

import scala.Tuple2;

/**
 * CellRDD to interact with mongoDB
 */
public final class MongoCellExtractor extends MongoExtractor<Cells> {

    /**
     * The constant LOG.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MongoCellExtractor.class);
    /**
     * The constant serialVersionUID.
     */
    private static final long serialVersionUID = -3208994171892747470L;

    /**
     * Instantiates a new Mongo cell extractor.
     */
    public MongoCellExtractor() {
        super();
        this.deepJobConfig = new MongoDeepJobConfig(Cells.class);
    }

    /**
     * Instantiates a new Mongo cell extractor.
     *
     * @param entityClass the entity class
     */
    public MongoCellExtractor(Class entityClass) {
        super();
        this.deepJobConfig = new MongoDeepJobConfig(entityClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cells transformElement(Tuple2<Object, BSONObject> tuple,
                                  DeepJobConfig<Cells, ? extends DeepJobConfig> config) {

        try {
            return UtilMongoDB.getCellFromBson(tuple._2(), deepJobConfig.getNameSpace());
        } catch (Exception e) {
            LOG.error("Cannot convert BSON: ", e);
            throw new DeepTransformException("Could not transform from Bson to Cell " + e.getMessage());
        }
    }

    @Override
    public Tuple2<Object, BSONObject> transformElement(Cells record) {
        return new Tuple2<>(null, UtilMongoDB.getBsonFromCell(record));
    }

}
