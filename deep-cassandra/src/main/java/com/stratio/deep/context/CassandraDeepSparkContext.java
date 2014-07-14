package com.stratio.deep.context;/*
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

import java.util.Map;

import com.stratio.deep.config.CellDeepJobConfig;
import com.stratio.deep.config.EntityDeepJobConfig;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.CassandraCellRDD;
import com.stratio.deep.rdd.CassandraEntityRDD;
import com.stratio.deep.rdd.CassandraJavaRDD;
import com.stratio.deep.rdd.CassandraRDD;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

/**
 * Created by luca on 11/07/14.
 */
public class CassandraDeepSparkContext extends DeepSparkContext {
	private static final Logger LOG = Logger.getLogger(CassandraDeepSparkContext.class);
	/**
	 * {@inheritDoc}
	 */
	public CassandraDeepSparkContext(SparkContext sc) {
		super(sc);
	}
	/**
	 * {@inheritDoc}
	 */
	public CassandraDeepSparkContext(String master, String appName) {
		super(master, appName);
	}

	/**
	 * {@inheritDoc}
	 */
	public CassandraDeepSparkContext(String master, String appName, String sparkHome, String jarFile) {
		super(master, appName, sparkHome, jarFile);
	}
	/**
	 * {@inheritDoc}
	 */
	public CassandraDeepSparkContext(String master, String appName, String sparkHome, String[] jars) {
		super(master, appName, sparkHome, jars);
	}
	/**
	 * {@inheritDoc}
	 */
	public CassandraDeepSparkContext(String master, String appName, String sparkHome, String[] jars, Map<String, String> environment) {
		super(master, appName, sparkHome, jars, environment);
	}

	/**
	 * Builds a new CassandraJavaRDD.
	 *
	 * @param config the deep configuration object to use to create the new RDD.
	 * @return a new CassandraJavaRDD
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> CassandraJavaRDD<T> cassandraJavaRDD(ICassandraDeepJobConfig<T> config) {
		return new CassandraJavaRDD<>(cassandraRDD(config));
	}

	/**
	 * Builds a new generic Cassandra RDD.
	 *
	 * @param config the deep configuration object to use to create the new RDD.
	 * @return a new generic CassandraRDD.
	 */
	@SuppressWarnings("unchecked")
	public <T> CassandraRDD<T> cassandraRDD(ICassandraDeepJobConfig<T> config) {
		if (config.getClass().isAssignableFrom(EntityDeepJobConfig.class)) {
			return new CassandraEntityRDD(sc(), config);
		}

		if (config.getClass().isAssignableFrom(CellDeepJobConfig.class)) {
			return (CassandraRDD<T>) new CassandraCellRDD(sc(), (ICassandraDeepJobConfig<com.stratio.deep.entity.Cells>) config);
		}

		throw new DeepGenericException("not recognized config type");

	}
}
