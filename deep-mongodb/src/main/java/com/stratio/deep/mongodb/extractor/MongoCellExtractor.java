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

import com.stratio.deep.commons.config.IDeepJobConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.mongodb.config.CellDeepJobConfigMongoDB;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

import scala.Tuple2;

/**
 * CellRDD to interact with mongoDB
 */
public final class MongoCellExtractor extends MongoExtractor<Cells> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoCellExtractor.class);
    private static final long serialVersionUID = -3208994171892747470L;

    public MongoCellExtractor() {
        super();
        this.deepJobConfig = new CellDeepJobConfigMongoDB();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cells transformElement(Tuple2<Object, BSONObject> tuple,
            IDeepJobConfig<Cells, ? extends IDeepJobConfig> config) {

        try {
            return UtilMongoDB.getCellFromBson(tuple._2());
        } catch (Exception e) {
            LOG.error("Cannot convert BSON: ", e);
            throw new DeepTransformException("Could not transform from Bson to Cell " + e.getMessage());
        }
    }

    @Override
    public Tuple2<Object, BSONObject> transformElement(Cells record) {
        try {
            return new Tuple2<>(null, UtilMongoDB.getBsonFromCell(record));
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            LOG.error(e.getMessage());
            throw new DeepTransformException(e.getMessage());
        }
    }

}
