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

import java.io.Serializable;

/**
 * Factory class for deep configuration objects.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class DeepJobConfigFactory implements Serializable {

    private static final long serialVersionUID = -4559130919203819088L;

    /**
     * private constructor
     */
    private DeepJobConfigFactory() {
    }

    /**
     * Creates a new cell-based job configuration object.
     *
     * @return
     */
    public static IDeepJobConfig<Cells> create() {
        return new CellDeepJobConfig(false);
    }

    /**
     * Creates a new cell-based write suitable job configuration object.
     *
     * @return
     */
    public static IDeepJobConfig<Cells> createWriteConfig() {
        return new CellDeepJobConfig(true);
    }

    /**
     * Creates an testentity-based configuration object.
     *
     * @return
     */
    public static <T extends IDeepType> IDeepJobConfig<T> create(Class<T> entityClass) {
        return new EntityDeepJobConfig<>(entityClass, false);
    }

    /**
     * Creates an testentity-based write configuration object.
     *
     * @return
     */
    public static <T extends IDeepType> IDeepJobConfig<T> createWriteConfig(Class<T> entityClass) {
        return new EntityDeepJobConfig<>(entityClass, true);
    }
}
