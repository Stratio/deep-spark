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

package com.stratio.deep.core.function;

import static com.stratio.deep.commons.utils.Utils.getExtractorInstance;
import static com.stratio.deep.core.util.ExtractorClientUtil.getExtractorClient;

import java.io.Serializable;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.exception.DeepExtractorInitializationException;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;

import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * Created by rcrespo on 28/08/14.
 */
public class PrepareSaveFunction<T, S extends BaseConfig> extends AbstractFunction1<Iterator<T>,
        BoxedUnit> implements Serializable {

    private static final long serialVersionUID = -7556596901637129564L;

    private final S config;

    private final T first;

    private final UpdateQueryBuilder queryBuilder;

    public PrepareSaveFunction(UpdateQueryBuilder queryBuilder, S config, T first) {
        this.first = first;
        this.config = config;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public BoxedUnit apply(Iterator<T> v1) {
        IExtractor<T, S> extractor;
        try {
            extractor = getExtractorInstance(config);
        } catch (DeepExtractorInitializationException e) {
            extractor = getExtractorClient();
        }

        extractor.initSave(config, first, queryBuilder);

        while (v1.hasNext()) {
            extractor.saveRDD(v1.next());
        }
        config.setPartitionId(config.getPartitionId() + 1);
        extractor.close();
        return null;
    }

}
