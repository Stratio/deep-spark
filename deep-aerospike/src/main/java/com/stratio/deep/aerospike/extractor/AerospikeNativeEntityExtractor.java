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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.Record;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.utils.UtilAerospike;
import com.stratio.deep.commons.exception.DeepTransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of AerospikeNativeExtractor for Stratio Deep Entity types.
 * @param <T> Type implementing IDeepType.
 */
public class AerospikeNativeEntityExtractor<T> extends AerospikeNativeExtractor<T, AerospikeDeepJobConfig<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeNativeEntityExtractor.class);
    private static final long serialVersionUID = 8071178758374072842L;

    /**
     * Public constructor.
     * @param t
     */
    public AerospikeNativeEntityExtractor(Class<T> t) {
        this.aerospikeDeepJobConfig = new AerospikeDeepJobConfig<>(t);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected T transformElement(Record record) {
        try {
            return (T) UtilAerospike.getObjectFromRecord(aerospikeDeepJobConfig.getEntityClass(), record, aerospikeDeepJobConfig);
        } catch(Exception e) {
            LOG.error("Cannot convert AerospikeRecord: ", e);
            throw new DeepTransformException("Could not transform from AerospikeRecord to Entity " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Record transformElement(T entity) {
        try {
            return UtilAerospike.getRecordFromObject(entity);
        } catch(Exception e) {
            LOG.error("Cannot convert entity to Aerospike Record: ", e);
            throw new DeepTransformException(e.getMessage(), e);
        }
    }
}
