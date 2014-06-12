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

import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.rdd.mongodb.MongoRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;

/**
 * Entry point to the Cassandra-aware Spark context.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepSparkContextMongo extends JavaSparkContext {

    /**
     * Overridden superclass constructor.
     *
     * @param sc an already created spark context.
     */
    public DeepSparkContextMongo(SparkContext sc) {
        super(sc);
    }


    /**
     * Builds a new CassandraJavaRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new CassandraJavaRDD
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> MongoJavaRDD<T> mongoJavaRDD(GenericDeepJobConfigMongoDB<T> config) {



            return new MongoJavaRDD(new MongoRDD(this.sc(), config));

    }

    public DeepSparkContextMongo(String master, String appName, String sparkHome, String jarFile) {
        super(master, appName, sparkHome, jarFile);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master the url of the master node.
     * @param appName the name of the application.
     * @param sparkHome the spark home folder.
     * @param jars the jar file(s) to serialize and send to all the cluster nodes.
     */
    public DeepSparkContextMongo(String master, String appName, String sparkHome, String[] jars) {
        super(master, appName, sparkHome, jars);
    }
}
