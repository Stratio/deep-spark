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

package com.stratio.deep.context;

import com.stratio.deep.config.CellDeepJobConfigMongoDB;
import com.stratio.deep.config.EntityDeepJobConfigMongoDB;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.mongodb.MongoCellRDD;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.DeepMongoRDD;

import java.util.Map;

/**
 * Created by luca on 11/07/14.
 */
public class MongoDeepSparkContext extends DeepSparkContext {
    private static final Logger LOG = Logger.getLogger(MongoDeepSparkContext.class);

    /**
     * {@inheritDoc}
     */
    public MongoDeepSparkContext(SparkContext sc) {
        super(sc);
    }

    /**
     * {@inheritDoc}
     */
    public MongoDeepSparkContext(String master, String appName) {
        super(master, appName);
    }

    /**
     * {@inheritDoc}
     */
    public MongoDeepSparkContext(String master, String appName, String sparkHome, String jarFile) {
        super(master, appName, sparkHome, jarFile);
    }

    /**
     * {@inheritDoc}
     */
    public MongoDeepSparkContext(String master, String appName, String sparkHome, String[] jars) {
        super(master, appName, sparkHome, jars);
    }

    /**
     * {@inheritDoc}
     */
    public MongoDeepSparkContext(String master, String appName, String sparkHome, String[] jars, Map<String, String> environment) {
        super(master, appName, sparkHome, jars, environment);
    }

    /**
     * Builds a new entity based MongoEntityRDD
     *
     * @param config
     * @param <T>
     * @return
     */
    public <T> JavaRDD<T> mongoJavaRDD(IMongoDeepJobConfig<T> config) {
        return new MongoJavaRDD<T>(mongoRDD(config));
    }

    /**
     * Builds a new Mongo RDD.
     *
     * @param config
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> DeepMongoRDD<T> mongoRDD(IMongoDeepJobConfig<T> config) {
        if (EntityDeepJobConfigMongoDB.class.isAssignableFrom(config.getClass())) {
            return new MongoEntityRDD(sc(), config);
        }

        if (CellDeepJobConfigMongoDB.class.isAssignableFrom(config.getClass())) {
            return (DeepMongoRDD<T>) new MongoCellRDD(sc(), (IMongoDeepJobConfig<Cells>) config);
        }

        throw new DeepGenericException("not recognized config type");

    }
}
