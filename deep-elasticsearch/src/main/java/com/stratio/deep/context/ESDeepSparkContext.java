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

import com.stratio.deep.config.CellDeepJobConfigES;
import com.stratio.deep.config.EntityDeepJobConfigES;
import com.stratio.deep.config.IESDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.ESCellRDD;
import com.stratio.deep.rdd.ESEntityRDD;
import com.stratio.deep.rdd.ESJavaRDD;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.DeepElasticSearchRDD;

import java.util.Map;

/**
 * Created by rcrespo on 2/07/14.
 */
public class ESDeepSparkContext extends DeepSparkContext {
    private static final Logger LOG = Logger.getLogger(ESDeepSparkContext.class);

    /**
     * {@inheritDoc}
     */
    public ESDeepSparkContext(SparkContext sc) {
        super(sc);
    }

    /**
     * {@inheritDoc}
     */
    public ESDeepSparkContext(String master, String appName) {
        super(master, appName);
    }

    /**
     * {@inheritDoc}
     */
    public ESDeepSparkContext(String master, String appName, String sparkHome, String jarFile) {
        super(master, appName, sparkHome, jarFile);
    }

    /**
     * {@inheritDoc}
     */
    public ESDeepSparkContext(String master, String appName, String sparkHome, String[] jars) {
        super(master, appName, sparkHome, jars);
    }

    /**
     * {@inheritDoc}
     */
    public ESDeepSparkContext(String master, String appName, String sparkHome, String[] jars, Map<String, String> environment) {
        super(master, appName, sparkHome, jars, environment);
    }

    /**
     * Builds a new entity based ESJavaRDD
     *

     * @return
     */
    public <T>JavaRDD<T> elasticJavaRDD(IESDeepJobConfig<T> config) {
        return new ESJavaRDD<T>(ESRDD(config));
    }

    /**
     * Builds a new ES RDD.
     *

     * @return
     */
    @SuppressWarnings("unchecked")
    public <T>DeepElasticSearchRDD<T> ESRDD(IESDeepJobConfig<T> config) {
        if (EntityDeepJobConfigES.class.isAssignableFrom(config.getClass())) {
            return new ESEntityRDD(this.sc(), config);
        }

        if (CellDeepJobConfigES.class.isAssignableFrom(config.getClass())) {
            return (DeepElasticSearchRDD<T>) new ESCellRDD(this.sc(), (IESDeepJobConfig<Cells>) config);
        }

        throw new DeepGenericException("not recognized config type");

    }

}
