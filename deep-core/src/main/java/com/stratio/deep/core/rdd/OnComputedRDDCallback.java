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
package com.stratio.deep.core.rdd;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.rdd.IExtractor;

import scala.runtime.AbstractFunction0;

/**
 * Helper callback class called by Spark when the current RDD is computed successfully. This class
 * simply closes the {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader} passed as an
 * argument.
 *
 * @param <T>
 * @author Luca Rosellini <luca@strat.io>
 */
public class OnComputedRDDCallback<T, S extends BaseConfig<T>> extends AbstractFunction0<T> {
    private final IExtractor<T, S> extractorClient;

    public OnComputedRDDCallback(IExtractor<T, S> extractorClient) {
        super();
        this.extractorClient = extractorClient;
    }

    @Override
    public T apply() {
        extractorClient.close();

        return null;
    }

}

