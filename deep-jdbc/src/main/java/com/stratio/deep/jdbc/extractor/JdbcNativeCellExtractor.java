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
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.utils.UtilJdbc;

import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcNativeCellExtractor extends JdbcNativeExtractor<Cells, JdbcDeepJobConfig<Cells>> {

    private static final long serialVersionUID = 5796562363902015583L;

    public JdbcNativeCellExtractor() {
        this.jdbcDeepJobConfig = new JdbcDeepJobConfig<>(Cells.class);
    }

    public JdbcNativeCellExtractor(Class<Cells> cellsClass) {
        this.jdbcDeepJobConfig = new JdbcDeepJobConfig<>(Cells.class);
    }

    @Override
    protected Cells transformElement(Map<String, Object> entity) {
        return UtilJdbc.getCellsFromObject(entity, jdbcDeepJobConfig);
    }

    @Override
    protected Map<String, Object> transformElement(Cells cells) {
        return UtilJdbc.getObjectFromCells(cells);
    }

}
