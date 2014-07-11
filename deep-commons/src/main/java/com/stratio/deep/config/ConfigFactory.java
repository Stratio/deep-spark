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

package com.stratio.deep.config;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.utils.DeepConfig;
import org.apache.log4j.Logger;

/**
 * Factory class for deep configuration objects.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class ConfigFactory implements Serializable {

    private static final long serialVersionUID = -4559130919203819088L;

    private static final Logger LOG = Logger.getLogger(ConfigFactory.class);

    /**
     * private constructor
     */
    private ConfigFactory() {
    }

    /**
     * Creates a new cell-based job configuration object.
     *
     * @return a new cell-based job configuration object.
     */
    public static ICassandraDeepJobConfig<Cells> create() {
        try{
            Class c = Class.forName(DeepConfig.CASSANDRA_CELL.getConfig());
            return  (ICassandraDeepJobConfig) c.getConstructors()[0].newInstance(false);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            throw new DeepGenericException("impossible to make a new instance, please check dependencies " +e.getMessage());
        }
    }

    /**
     * Creates a new cell-based write suitable job configuration object.
     *
     * @return a new cell-based write suitable job configuration object.
     */
    public static ICassandraDeepJobConfig<Cells> createWriteConfig() {
        try{
            Class c = Class.forName(DeepConfig.CASSANDRA_CELL.getConfig());
            return  (ICassandraDeepJobConfig) c.getConstructors()[0].newInstance(true);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            throw new DeepGenericException("impossible to make a new instance, please check dependencies " +e.getMessage());
        }
    }

    /**
     * Creates an entity-based configuration object.
     *
     * @param entityClass the class instance of the entity class that will be used to map db objects to Java objects.
     * @param <T> the generic type of the entity object implementing IDeepType.
     * @return a new an entity-based configuration object.
     */
    public static <T extends IDeepType> ICassandraDeepJobConfig<T> create(Class<T> entityClass) {
        try{
            Class c = Class.forName(DeepConfig.CASSANDRA_ENTITY.getConfig());
            return  (ICassandraDeepJobConfig) c.getConstructors()[0].newInstance(entityClass, false);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            throw new DeepGenericException("impossible to make a new instance, please check dependencies " +e.getMessage());
        }
    }

    /**
     * Creates an entity-based write configuration object.
     *
     * @return an entity-based write configuration object.
     */
    public static <T extends IDeepType> ICassandraDeepJobConfig<T> createWriteConfig(Class<T> entityClass) {
        try{
            Class c = Class.forName(DeepConfig.CASSANDRA_ENTITY.getConfig());
            return  (ICassandraDeepJobConfig) c.getConstructors()[0].newInstance(entityClass, true);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            throw new DeepGenericException("impossible to make a new instance, please check dependencies " +e.getMessage());
        }
    }


    /**
	 * Creates a new cell-based MongoDB job configuration object.
	 *
	 * @return a new cell-based MongoDB job configuration object.
	 */
    public static IMongoDeepJobConfig<Cells> createMongoDB() {
        try{
            Class c = Class.forName(DeepConfig.MONGODB_CELL.getConfig());
            return  (IMongoDeepJobConfig) c.getConstructors()[0].newInstance();
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            throw new DeepGenericException("impossible to make a new instance, please check dependencies " +e.getMessage());
        }
    }

	/**
	 * Creates a new entity-based MongoDB job configuration object.
	 *
	 * @param entityClass the class instance of the entity class that will be used to map db objects to Java objects.
	 * @param <T> the generic type of the entity object implementing IDeepType.
	 * @return a new entity-based MongoDB job configuration object.
	 */
    public static <T extends IDeepType> IMongoDeepJobConfig<T> createMongoDB(Class<T> entityClass) {
        try{
            Class c = Class.forName(DeepConfig.MONGODB_ENTITY.getConfig());
            return  (IMongoDeepJobConfig) c.getConstructors()[0].newInstance(entityClass);
        }catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e){
            LOG.error(e.getMessage());
            throw new DeepGenericException("impossible to make a new instance, please check dependencies " +e.getMessage());
        }
    }
}
