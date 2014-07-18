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
import com.stratio.deep.entity.Cells;
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
 * CellRDD to interact with mongoDB
 */
public final class MongoCellRDD extends DeepMongoRDD<Cells> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoCellRDD.class);
    private static final long serialVersionUID = -3208994171892747470L;


    /**
     * Public constructor that builds a new MongoCellRDD RDD given the context and the configuration file.
     *
     * @param sc     the spark context to which the RDD will be bound to.
     * @param config the deep configuration object.
     */
    @SuppressWarnings("unchecked")
    public MongoCellRDD(SparkContext sc, IMongoDeepJobConfig<Cells> config) {
        super(sc, config, ClassTag$.MODULE$.<Cells>apply(config.getEntityClass()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cells transformElement(Tuple2<Object, BSONObject> tuple) {


        try {
            return UtilMongoDB.getCellFromBson(tuple._2());
        } catch (Exception e) {
            LOG.error("Cannot convert BSON: ", e);
            throw new DeepTransformException("Could not transform from Bson to Cell " + e.getMessage());
        }
    }


    /**
     * Save a RDD to MongoDB
     *
     * @param rdd
     * @param config
     */
    public static void saveCell(RDD rdd, IMongoDeepJobConfig<Cells> config) {

        JavaPairRDD<Object, BSONObject> save = rdd.toJavaRDD().mapToPair(new PairFunction<Cells, Object, BSONObject>() {


            @Override
            public Tuple2<Object, BSONObject> call(Cells t) throws Exception {
                return new Tuple2<>(null, UtilMongoDB.getBsonFromCell(t));
            }
        });


        // Only MongoOutputFormat and config are relevant
        save.saveAsNewAPIHadoopFile("file:///cell", Object.class, Object.class, MongoOutputFormat.class, config.getHadoopConfiguration());
    }

}
