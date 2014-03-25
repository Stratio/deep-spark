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

import java.util.Map;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.impl.CellDeepJobConfig;
import com.stratio.deep.config.impl.EntityDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.CassandraCellRDD;
import com.stratio.deep.rdd.CassandraEntityRDD;
import com.stratio.deep.rdd.CassandraJavaRDD;
import com.stratio.deep.rdd.CassandraRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Entry point to the Cassandra-aware Spark context.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepSparkContext extends JavaSparkContext {

    /**
     * Overridden superclass constructor.
     *
     * @param sc
     */
    public DeepSparkContext(SparkContext sc) {
        super(sc);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master
     * @param appName
     */
    public DeepSparkContext(String master, String appName) {
        super(master, appName);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master
     * @param appName
     * @param sparkHome
     * @param jarFile
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String jarFile) {
        super(master, appName, sparkHome, jarFile);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master
     * @param appName
     * @param sparkHome
     * @param jars
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String[] jars) {
        super(master, appName, sparkHome, jars);
    }

    /**
     * Overridden superclass constructor.
     *
     * @param master
     * @param appName
     * @param sparkHome
     * @param jars
     * @param environment
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String[] jars,
        Map<String, String> environment) {
        super(master, appName, sparkHome, jars, environment);
    }

    /**
     * Builds a new CassandraEntityRDD.
     *
     * @param config
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> CassandraJavaRDD<T> cassandraJavaRDD(IDeepJobConfig<T> config) {
        if (config instanceof EntityDeepJobConfig) {
            return new CassandraJavaRDD<T>(cassandraEntityRDD((EntityDeepJobConfig) config));
        }

        if (config instanceof CellDeepJobConfig) {
            return new CassandraJavaRDD<T>((CassandraRDD<T>) cassandraGenericRDD((CellDeepJobConfig) config));
        }

        throw new DeepGenericException("not recognized config type");
    }

    /**
     * Builds a new entity based CassandraEntityRDD.
     *
     * @param config
     * @return
     */
    public <T extends IDeepType> CassandraRDD<T> cassandraEntityRDD(IDeepJobConfig<T> config) {
        return new CassandraEntityRDD<T>(sc(), config);
    }

    /**
     * Builds a new generic (cell based) CassandraEntityRDD.
     *
     * @param config
     * @return
     */
    public CassandraRDD<Cells> cassandraGenericRDD(IDeepJobConfig<Cells> config) {
        return new CassandraCellRDD(sc(), config);
    }
}
