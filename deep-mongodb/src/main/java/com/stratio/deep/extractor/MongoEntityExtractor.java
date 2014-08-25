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

package com.stratio.deep.extractor;

import com.mongodb.hadoop.MongoOutputFormat;
import com.stratio.deep.config.*;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepTransformException;
import com.stratio.deep.rdd.IExtractor;
import com.stratio.deep.utils.UtilMongoDB;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * EntityRDD to interact with mongoDB
 *
 * @param <T>
 */
public final class MongoEntityExtractor<T> extends MongoExtractor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoEntityExtractor.class);
    private static final long serialVersionUID = -3208994171892747470L;



    public MongoEntityExtractor(T t){
        super();
        this.mongoJobConfig = new EntityDeepJobConfigMongoDB(t.getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T transformElement(Tuple2<Object, BSONObject> tuple, IMongoDeepJobConfig<T> config ) {


        try {
            return UtilMongoDB.getObjectFromBson(config.getEntityClass(), tuple._2());
        } catch (Exception e) {
            LOG.error("Cannot convert BSON: ", e);
            throw new DeepTransformException("Could not transform from Bson to Entity " + e.getMessage());
        }

    }



    @Override
    public IExtractor getExtractorInstance(ExtractorConfig<T> config) {
        return this;
    }

    /**
     * Save a RDD to MongoDB
     *
     * @param rdd
     * @param config
     */
    public void saveRDD(RDD<T> rdd, ExtractorConfig<T> config) {

        mongoJobConfig = mongoJobConfig.initialize(config);

        JavaPairRDD<Object, BSONObject> save = rdd.toJavaRDD().mapToPair(new PairFunction<T, Object, BSONObject>() {


            @Override
            public Tuple2<Object, BSONObject> call(T t) throws Exception {
                return new Tuple2<>(null, UtilMongoDB.getBsonFromObject(t));
            }
        });


        // Only MongoOutputFormat and config are relevant
        save.saveAsNewAPIHadoopFile("file:///entity", Object.class, Object.class, MongoOutputFormat.class, mongoJobConfig.getHadoopConfiguration());
    }


}
