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
package com.stratio.deep.core.fs.utils;

import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.utils.Utils;

public class MapSchemaFromLines implements Function<String, Cells> {

    /**
     * Columns
     */
    private final List<SchemaMap<?>> columns;

    /**
     * Separator in the FS File
     */
    private String separator;

    /**
     * TableName of the FS File
     */
    private String tableName;

    /**
     * Catalog of the FS File
     */
    private String catalogName;

    public MapSchemaFromLines(TextFileDataTable textFileDataTable) {
        this.tableName = textFileDataTable.getTableName().getTableName();
        this.catalogName = textFileDataTable.getTableName().getCatalogName();
        this.columns = textFileDataTable.getColumnMap();
        this.separator = textFileDataTable.getLineSeparator();
    }

    @Override
    public Cells call(String s) throws Exception {

        Cells cellsOut = new Cells(catalogName + "." + tableName);

        String[] splits = s.split(separator);

        int i = 0;
        for (SchemaMap schemaMap : columns) {

            Cell cell = Cell.create(schemaMap.getFieldName(), Utils.castingUtil(splits[i], schemaMap.getFieldType()));
            cellsOut.add(cell);
            i++;
        }

        return cellsOut;
    }

}
