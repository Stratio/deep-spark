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

package com.stratio.deep.rdd;

import com.stratio.deep.config.CellDeepJobConfig;
import com.stratio.deep.config.EntityDeepJobConfig;
import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.CassandraCell;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.utils.Pair;
import com.stratio.deep.utils.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.spark.rdd.RDD;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Stratio's implementation of an RDD reading and writing data from and to Apache Cassandra. This
 * implementation uses Cassandra's Hadoop API.
 * <p/>
 * We do not use Map<String,ByteBuffer> as key and value objects, since ByteBuffer is not
 * serializable.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class CassandraEntityExtractor<T extends IDeepType> extends CassandraExtractor<T> {

    private static final long serialVersionUID = -3208994171892747470L;


    public CassandraEntityExtractor(T t){
        super();
        this.cassandraJobConfig = new EntityDeepJobConfig(t.getClass(), true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem,
                              IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {
        Map<String, Cell> columnDefinitions = config.columnDefinitions();

        Class<T> entityClass = config.getEntityClass();

        EntityDeepJobConfig<T> edjc = (EntityDeepJobConfig) config;
        T instance = Utils.newTypeInstance(entityClass);

        for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
            CassandraCell metadata = (CassandraCell) columnDefinitions.get(entry.getKey());
            AbstractType<?> marshaller = metadata.marshaller();
            edjc.setInstancePropertyFromDbName(instance, entry.getKey(),
                    marshaller.compose(entry.getValue()));
        }

        for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            CassandraCell metadata = (CassandraCell) columnDefinitions.get(entry.getKey());
            AbstractType<?> marshaller = metadata.marshaller();
            try {
                edjc.setInstancePropertyFromDbName(instance, entry.getKey(),
                        marshaller.compose(entry.getValue()));
            } catch (DeepNoSuchFieldException e) {
                // log().debug(e.getMessage());
            }
        }

        return instance;
    }

    @Override
    public Class getConfigClass() {
        return EntityDeepJobConfig.class;
    }

    @Override
    public IExtractor<T> getExtractorInstance(ExtractorConfig<T> config) {
        return null;
    }

    @Override
    public void saveRDD(RDD<T> rdd, ExtractorConfig<T> config) {

    }
}
