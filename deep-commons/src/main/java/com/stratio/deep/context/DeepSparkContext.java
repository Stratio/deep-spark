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


import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.utils.DeepConfig;
import com.stratio.deep.utils.DeepRDD;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

/**
 * Entry point to the Cassandra-aware Spark context.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepSparkContext extends JavaSparkContext {

    private static final Logger LOG = Logger.getLogger(DeepSparkContext.class);


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




    //Cassandra
    /**
     * Builds a new CassandraJavaRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new CassandraJavaRDD
     */
//    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T, S extends IDeepJobConfig> JavaRDD<T> cassandraJavaRDD(IDeepJobConfig<T, S> config) {
        try {

            Class c = Class.forName(DeepRDD.CASSANDRA_JAVA.getClassName());;

            if (config.getClass().isAssignableFrom(Class.forName(DeepConfig.CASSANDRA_ENTITY.getConfig()))) {
                return (JavaRDD<T>) c.getConstructors()[0].newInstance(cassandraEntityRDD((ICassandraDeepJobConfig) config));
            }

            if (config.getClass().isAssignableFrom(Class.forName(DeepConfig.CASSANDRA_CELL.getConfig()))) {
                return (JavaRDD<T>) c.getConstructors()[0].newInstance(cassandraGenericRDD((ICassandraDeepJobConfig) config));

            }

            throw new DeepGenericException("not recognized config type");
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            LOG.error(e.getMessage());
            return null;
        }
    }

    /**
     * Builds a new testentity based CassandraEntityRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new entity-based CassandraRDD
     */
    public <T extends IDeepType> RDD<T> cassandraEntityRDD(ICassandraDeepJobConfig<T> config) {
        try{
            Class c = Class.forName(DeepRDD.CASSANDRA_ENTITY.getClassName());
            return  (RDD<T>) c.getConstructors()[0].newInstance(sc(), config);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            return null;
        }
    }

    /**
     * Builds a new generic (cell based) CassandraGenericRDD.
     *
     * @param config the deep configuration object to use to create the new RDD.
     * @return a new generic CassandraRDD.
     */
    public RDD<Cells> cassandraGenericRDD(ICassandraDeepJobConfig<Cells> config) {
        try{
            Class c = Class.forName(DeepRDD.CASSANDRA_CELL.getClassName());
            return  (RDD<Cells>) c.getConstructors()[0].newInstance(sc(), config);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            return null;
        }
    }


    //MongoDB


    /**
     * Builds a new entity based MongoEntityRDD
     *
     * @param config
     * @param <T>
     * @return
     */
    public <T extends IDeepType> JavaRDD<T> mongoJavaRDD(IMongoDeepJobConfig<T> config) {
        try{
            Class c = Class.forName(DeepRDD.MONGO_JAVA.getClassName());
            return  (JavaRDD<T>) c.getConstructors()[0].newInstance(mongoEntityRDD(config));
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            return null;
        }

    }

    /**
     * Builds a new entity based MongoEntityRDD
     *
     * @param config
     * @param <T>
     * @return
     */

    public <T extends IDeepType> RDD<T> mongoEntityRDD(IMongoDeepJobConfig<T> config) {
        try{
            Class c = Class.forName(DeepRDD.MONGO_ENTITY.getClassName());
            return  (RDD<T>) c.getConstructors()[0].newInstance(sc(), config);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            return null;
        }

    }

    /**
     * Builds a new cell based MongoEntityRDD
     *
     * @param config
     * @param <T>
     * @return
     */
//    com.stratio.deep.rdd.mongodb.MongoCellRDD
    public <T extends IDeepType> RDD mongoCellRDD(IMongoDeepJobConfig<Cells> config) {
        try{
            Class c = Class.forName(DeepRDD.MONGO_CELL.getClassName());
            return  (RDD<T>) c.getConstructors()[0].newInstance(sc(), config);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            return null;
        }
    }

}
