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

import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import org.apache.log4j.Logger;

/**
 * Created by luca on 14/07/14.
 */
public class MongoConfigFactory {
    private static final Logger LOG = Logger.getLogger(MongoConfigFactory.class);

    private MongoConfigFactory() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a new cell-based MongoDB job configuration object.
     *
     * @return a new cell-based MongoDB job configuration object.
     */
    public static IMongoDeepJobConfig<Cells> createMongoDB() {
        return new CellDeepJobConfigMongoDB();
    }

    /**
     * Creates a new entity-based MongoDB job configuration object.
     *
     * @param entityClass the class instance of the entity class that will be used to map db objects to Java objects.
     * @param <T>         the generic type of the entity object implementing IDeepType.
     * @return a new entity-based MongoDB job configuration object.
     */
    public static <T extends IDeepType> IMongoDeepJobConfig<T> createMongoDB(Class<T> entityClass) {
        return new EntityDeepJobConfigMongoDB<>(entityClass);
    }
}
