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

package com.stratio.deep.cassandra.config;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Factory class for deep configuration objects.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class CassandraConfigFactory implements Serializable {

    private static final long serialVersionUID = -4559130919203819088L;

    private static final Logger LOG = Logger.getLogger(CassandraConfigFactory.class);

    /**
     * private constructor
     */
    private CassandraConfigFactory() {
    }

    /**
     * Creates a new cell-based job configuration object.
     *
     * @return a new cell-based job configuration object.
     */
    public static CassandraDeepJobConfig<Cells> create() {
        return new CellDeepJobConfig();
    }

    /**
     * Creates a new cell-based write suitable job configuration object.
     *
     * @return a new cell-based write suitable job configuration object.
     */
    public static CassandraDeepJobConfig<Cells> createWriteConfig() {
        return new CellDeepJobConfig();
    }

    /**
     * Creates an entity-based configuration object.
     *
     * @param entityClass the class instance of the entity class that will be used to map db objects to Java objects.
     * @param <T>         the generic type of the entity object implementing IDeepType.
     * @return a new an entity-based configuration object.
     */
    public static <T extends IDeepType> CassandraDeepJobConfig<T> create(Class<T> entityClass) {
        return new EntityDeepJobConfig<>(entityClass);
    }

    /**
     * Creates an entity-based write configuration object.
     *
     * @return an entity-based write configuration object.
     */
    public static <T extends IDeepType> CassandraDeepJobConfig<T> createWriteConfig(Class<T> entityClass) {
        return new EntityDeepJobConfig<>(entityClass);
    }

}
