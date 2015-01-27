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
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import com.stratio.deep.jdbc.reader.JdbcNeo4JReader;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;

import java.util.Map;

import static com.stratio.deep.commons.utils.Utils.initConfig;

/**
 * Abstract class of Jdbc for Neo4J native extractor.
 */
public abstract class JdbcNeo4JNativeExtractor<T, S extends BaseConfig> extends JdbcNativeExtractor<T, S> {

    private static final long serialVersionUID = 7943398035972415583L;

    /**
     * JDBC for Neo4J extractor configuration.
     */
    protected JdbcNeo4JDeepJobConfig<T> jdbcNeo4JDeepJobConfig;

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition[] getPartitions(S config){
        JdbcNeo4JDeepJobConfig neo4jConfig = (JdbcNeo4JDeepJobConfig)config;
        JdbcPartition partition = new JdbcPartition(0, neo4jConfig.getLowerBound(), neo4jConfig.getUpperBound());
        Partition [] result = new Partition[1];
        result[0] = partition;
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initIterator(Partition dp, S config) {
        jdbcNeo4JDeepJobConfig = initConfig(config, jdbcNeo4JDeepJobConfig);
        this.jdbcReader = new JdbcNeo4JReader(jdbcNeo4JDeepJobConfig);
        try {
            this.jdbcReader.init(dp);
        } catch(Exception e) {
            throw new DeepGenericException("Unable to initialize JdbcReader", e);
        }
    }

    @Override
    protected abstract T transformElement(Map<String, Object> entity);

    @Override
    protected abstract Map<String, Object> transformElement(T entity);

}
