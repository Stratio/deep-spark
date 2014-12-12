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

package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public abstract class JdbcNativeExtractor<T, S extends BaseConfig<T>> implements IExtractor<T, S> {

    private static final long serialVersionUID = -298383130965427783L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcNativeExtractor.class);

    protected JdbcDeepJobConfig<T> jdbcDeepJobConfig;

    protected JdbcReader jdbcReader;

    protected JdbcWriter<T> jdbcWriter;

    @Override
    public Partition[] getPartitions(S config) {
        initJdbcDeepJobConfig(config);
        int upperBound = jdbcDeepJobConfig.getUpperBound();
        int lowerBound = jdbcDeepJobConfig.getLowerBound();
        int numPartitions = jdbcDeepJobConfig.getNumPartitions();
        int length = 1 + upperBound - lowerBound;
        Partition [] result = new Partition[numPartitions];
        for(int i=0; i<numPartitions; i++) {
            int start = lowerBound + lowerBound + ((i * length) / numPartitions);
            int end = lowerBound + (((i + 1) * length) / numPartitions) - 1;
            result[i] = new JdbcPartition(i, start, end);
        }
        return result;
    }

    @Override
    public boolean hasNext() {
        try {
            return jdbcReader.hasNext();
        } catch (SQLException e) {
            throw new DeepGenericException(e);
        }
    }

    @Override
    public T next() {
        try {
            return transformElement(jdbcReader.next());
        } catch (SQLException e) {
            throw new DeepGenericException(e);
        }
    }

    @Override
    public void close() {
        if(jdbcReader != null) {
            try {
                jdbcReader.close();
            } catch(SQLException e) {
                LOG.error("Unable to close jdbcReader", e);
            }
        }
        if(jdbcWriter != null) {
            try {
                jdbcWriter.close();
            } catch(SQLException e) {
                LOG.error("Unable to close jdbcWriter", e);
            }
        }
    }

    @Override
    public void initIterator(Partition dp, S config) {
        initJdbcDeepJobConfig(config);
        this.jdbcReader = new JdbcReader(jdbcDeepJobConfig);
        try {
            this.jdbcReader.init(dp);
        } catch(Exception e) {
            throw new DeepGenericException("Unable to initialize JdbcReader", e);
        }
    }

    @Override
    public void saveRDD(T t) {
        try {
            this.jdbcWriter.save(transformElement(t));
        } catch(Exception e) {
            throw new DeepGenericException("Error while writing row", e);
        }
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return null;
    }

    @Override
    public void initSave(S config, T first, UpdateQueryBuilder queryBuilder) {
        initJdbcDeepJobConfig(config);
        try {
            this.jdbcWriter = new JdbcWriter<>(jdbcDeepJobConfig);
        } catch(Exception e) {
            throw new DeepGenericException(e);
        }
    }

    private void initJdbcDeepJobConfig(S config) {
        if (config instanceof ExtractorConfig) {
            jdbcDeepJobConfig.initialize((ExtractorConfig) config);
        } else {
            jdbcDeepJobConfig = (JdbcDeepJobConfig) config;
        }
    }

    /**
     * Transform element.
     *
     * @param entity the entity
     * @return the dB object
     */
    protected abstract T transformElement(Map<String, Object> entity);

    protected abstract Map<String, Object> transformElement(T entity);
}
