/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.stratio.deep.context;


import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepInstantiationException;
import com.stratio.deep.rdd.DeepJavaRDD;
import com.stratio.deep.rdd.DeepRDD;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Entry point to the Cassandra-aware Spark context.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public class DeepSparkContext extends JavaSparkContext implements Serializable {

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


    /**
     * @param deepJobConfig
     * @param <T>
     * @return
     */
    public <T> RDD<T> createRDD(final DeepJobConfig deepJobConfig) {
        try {

            Class rdd = deepJobConfig.getRDDClass();
            final Constructor c = rdd.getConstructor();
            if (deepJobConfig.getInputFormatClass() != null) {
                return null;
//        return new DeepGenericHadoopRDD(this.sc(), deepJobConfig, deepJobConfig.getInputFormatClass(),
//            (IDeepHadoopRDD) c.newInstance(), ClassTag$.MODULE$.<Cells>apply(deepJobConfig
//                .getEntityClass()), ClassTag$.MODULE$.Any(), ClassTag$.MODULE$.Any());
            } else {

                return new DeepRDD<T>(this.sc(), deepJobConfig);
            }

        } catch (NoSuchMethodException e) {
            LOG.error("impossible to instance IDeepJobConfig, check configuration");
            throw new DeepInstantiationException(e.getMessage());
        }

    }

    /**
     * @param deepJobConfig
     * @param <T>
     * @return
     */
    public <T> JavaRDD<T> createJavaRDD(
            DeepJobConfig<T> deepJobConfig) {
        return new DeepJavaRDD((DeepRDD<T>) createRDD(deepJobConfig));
    }


    public <T, S extends IDeepJobConfig<?, ?>> void saveRDD(RDD<T> rdd,
                                                            IDeepJobConfig<T, S> deepJobConfig) {
        try {
            deepJobConfig.getSaveMethod().invoke(null, rdd, deepJobConfig);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new DeepInstantiationException(e.getMessage());
        }
    }
}