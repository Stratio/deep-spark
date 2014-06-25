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

import com.stratio.deep.config.*;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.CassandraCellRDD;
import com.stratio.deep.rdd.CassandraEntityRDD;
import com.stratio.deep.rdd.CassandraJavaRDD;
import com.stratio.deep.rdd.CassandraRDD;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * Entry point to the Cassandra-aware Spark context.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepSparkContext extends JavaSparkContext {

    /**
     * Overridden superclass constructor.
     *
     * @param sc an already created spark context.
     */
    public DeepSparkContext(SparkContext sc) {
        super(sc);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master  the url of the master node.
     * @param appName the name of the application.
     */
    public DeepSparkContext(String master, String appName) {
        super(master, appName);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master    the url of the master node.
     * @param appName   the name of the application.
     * @param sparkHome the spark home folder.
     * @param jarFile   the jar file to serialize and send to all the cluster nodes.
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String jarFile) {
        super(master, appName, sparkHome, jarFile);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master    the url of the master node.
     * @param appName   the name of the application.
     * @param sparkHome the spark home folder.
     * @param jars      the jar file(s) to serialize and send to all the cluster nodes.
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String[] jars) {
        super(master, appName, sparkHome, jars);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master      the url of the master node.
     * @param appName     the name of the application.
     * @param sparkHome   the spark home folder.
     * @param jars        the jar file(s) to serialize and send to all the cluster nodes.
     * @param environment a map of environment variables.
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String[] jars,
                            Map<String, String> environment) {
        super(master, appName, sparkHome, jars, environment);
    }

    /**
     * Builds a new CassandraJavaRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new CassandraJavaRDD
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T, S extends IDeepJobConfig> CassandraJavaRDD<T> cassandraJavaRDD(IDeepJobConfig<T, S> config) {
        if (config instanceof EntityDeepJobConfig) {
            return new CassandraJavaRDD<T>(cassandraEntityRDD((EntityDeepJobConfig) config));
        }

        if (config instanceof CellDeepJobConfig) {
            return new CassandraJavaRDD<T>((CassandraRDD<T>) cassandraGenericRDD((CellDeepJobConfig) config));
        }

        throw new DeepGenericException("not recognized config type");
    }

    /**
     * Builds a new testentity based CassandraEntityRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new entity-based CassandraRDD
     */
    public <T extends IDeepType> CassandraRDD<T> cassandraEntityRDD(ICassandraDeepJobConfig<T> config) {
        return new CassandraEntityRDD<T>(sc(), config);
    }

    /**
     * Builds a new generic (cell based) CassandraGenericRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new generic CassandraRDD.
     */
    public CassandraRDD<Cells> cassandraGenericRDD(ICassandraDeepJobConfig<Cells> config) {
        return new CassandraCellRDD(sc(), config);
    }

    /**
     * Builds a new entity based MongoEntityRDD
     *
     * @param config
     * @param <T>
     * @return
     */
    public <T> MongoJavaRDD<T> mongoJavaRDD(IMongoDeepJobConfig<T> config) {
        return new MongoJavaRDD<T>(new MongoEntityRDD(this.sc(), (EntityDeepJobConfigMongoDB) config));

    }

}
