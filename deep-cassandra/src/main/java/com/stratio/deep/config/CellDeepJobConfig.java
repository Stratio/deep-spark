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
import com.stratio.deep.rdd.CassandraCellExtractor;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Method;

/**
 * Cell-based configuration object.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class CellDeepJobConfig extends GenericDeepJobConfig<Cells> {

    private static final long serialVersionUID = -598862509865396541L;
    private Cells dummyCells;

    public CellDeepJobConfig(Boolean isWriteConfig) {
        this.isWriteConfig = isWriteConfig;
    }

    {
        dummyCells = new Cells("dummyCellsTable");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Cells> getEntityClass() {
        return (Class<Cells>) dummyCells.getClass();
    }

    @Override
    public Class<?> getRDDClass() {
        return CassandraCellExtractor.class;
    }


    public Method getSaveMethod() throws NoSuchMethodException {
        return null;
    }


    public Configuration getHadoopConfiguration() {
        return null;
    }





}
