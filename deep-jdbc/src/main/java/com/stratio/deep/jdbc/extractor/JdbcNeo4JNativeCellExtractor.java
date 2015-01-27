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

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import com.stratio.deep.jdbc.utils.UtilJdbc;

import java.util.Map;

/**
 * Implementation of JdbcNeo4JNativeExtractor for Cells objects.
 */
public class JdbcNeo4JNativeCellExtractor extends JdbcNeo4JNativeExtractor<Cells, JdbcNeo4JDeepJobConfig> {

    /**
     * Default constructor.
     */
    public JdbcNeo4JNativeCellExtractor() {
        this.jdbcNeo4JDeepJobConfig = new JdbcNeo4JDeepJobConfig<>(Cells.class);
    }

    public JdbcNeo4JNativeCellExtractor(Class<Cells> cellsClass) {
        this.jdbcNeo4JDeepJobConfig = new JdbcNeo4JDeepJobConfig<>(cellsClass);
    }

    /**
     * Transforms a database row represented as a Map into a Cells object.
     * @param entity Database row represented as a Map of column name:column value.
     * @return Cells object with database row data.
     */
    @Override
    protected Cells transformElement(Map<String, Object> entity) {
        return UtilJdbc.getCellsFromObject(entity, this.jdbcNeo4JDeepJobConfig);
    }

    /**
     * Transforms a Cells object into a database row represented as a Map.
     * @param cells Cells data object.
     * @return Database row represented as a Map of column name:column value.
     */
    @Override
    protected Map<String, Object> transformElement(Cells cells) {
        return UtilJdbc.getObjectFromCells(cells);
    }
}
