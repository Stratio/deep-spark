/*
 * Copyright 2014, Stratio.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.stratio.deep.cassandra.extractor;

import java.nio.ByteBuffer;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.config.EntityDeepJobConfig;
import com.stratio.deep.cassandra.functions.DeepType2TupleFunction;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepNoSuchFieldException;
import com.stratio.deep.commons.utils.Pair;
import com.stratio.deep.commons.utils.Utils;

/**
 * Stratio's implementation of an RDD reading and writing data from and to Apache Cassandra. This
 * implementation uses Cassandra's Hadoop API.
 * <p/>
 * We do not use Map<String,ByteBuffer> as key and value objects, since ByteBuffer is not
 * serializable.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class CassandraEntityExtractor<T extends IDeepType> extends CassandraExtractor<T, EntityDeepJobConfig<T>> {

    private static final long serialVersionUID = -3208994171892747470L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraEntityExtractor.class);

    public CassandraEntityExtractor(Class<T> t) {
        super();
        this.cassandraJobConfig = new EntityDeepJobConfig(t);
        this.transformer = new DeepType2TupleFunction<T>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem,
                              CassandraDeepJobConfig<T> config) {
        Map<String, Cell> columnDefinitions = ((CassandraDeepJobConfig) config).columnDefinitions();

        Class<T> entityClass = config.getEntityClass();

        EntityDeepJobConfig<T> edjc = (EntityDeepJobConfig) config;
        T instance = Utils.newTypeInstance(entityClass);

        for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
            Cell metadata = columnDefinitions.get(entry.getKey());
            edjc.setInstancePropertyFromDbName(instance, entry.getKey(), ((DataType) metadata.getValue())
                    .deserialize(entry.getValue(),CassandraDeepJobConfig.PROTOCOL_VERSION));
        }

        for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            Cell metadata = columnDefinitions.get(entry.getKey());
            try {

                edjc.setInstancePropertyFromDbName(instance, entry.getKey(), ((DataType) metadata.getValue())
                        .deserialize(entry.getValue(),CassandraDeepJobConfig.PROTOCOL_VERSION));
            } catch (DeepNoSuchFieldException e) {
                LOG.error(e.getMessage());
            }
        }

        return instance;
    }

    @Override
    public Class getConfigClass() {
        return EntityDeepJobConfig.class;
    }

}
