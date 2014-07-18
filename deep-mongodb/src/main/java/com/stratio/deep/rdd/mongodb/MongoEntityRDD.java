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

package com.stratio.deep.rdd.mongodb;

import com.mongodb.hadoop.MongoOutputFormat;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepTransformException;
import com.stratio.deep.utils.UtilMongoDB;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.DeepMongoRDD;
import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag$;

/**
 * EntityRDD to interact with mongoDB
 *
 * @param <T>
 */
public final class MongoEntityRDD<T extends IDeepType> extends DeepMongoRDD<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoEntityRDD.class);
    private static final long serialVersionUID = -3208994171892747470L;


    /**
     * Public constructor that builds a new MongoEntityRDD RDD given the context and the configuration file.
     *
     * @param sc     the spark context to which the RDD will be bound to.
     * @param config the deep configuration object.
     */
    @SuppressWarnings("unchecked")
    public MongoEntityRDD(SparkContext sc, IMongoDeepJobConfig<T> config) {
        super(sc, config, ClassTag$.MODULE$.<T>apply(config.getEntityClass()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T transformElement(Tuple2<Object, BSONObject> tuple) {


        try {
            return UtilMongoDB.getObjectFromBson(this.getConf().getEntityClass(), tuple._2());
        } catch (Exception e) {
            LOG.error("Cannot convert BSON: ", e);
            throw new DeepTransformException("Could not transform from Bson to Entity " + e.getMessage());
        }

    }


    /**
     * Save a RDD to MongoDB
     *
     * @param rdd
     * @param config
     * @param <T>
     */
    public static <T extends IDeepType> void saveEntity(RDD<T> rdd, IMongoDeepJobConfig<T> config) {

        JavaPairRDD<Object, BSONObject> save = rdd.toJavaRDD().mapToPair(new PairFunction<T, Object, BSONObject>() {


            @Override
            public Tuple2<Object, BSONObject> call(T t) throws Exception {
                return new Tuple2<>(null, UtilMongoDB.getBsonFromObject(t));
            }
        });


        // Only MongoOutputFormat and config are relevant
        save.saveAsNewAPIHadoopFile("file:///entity", Object.class, Object.class, MongoOutputFormat.class, config.getHadoopConfiguration());
    }

}
