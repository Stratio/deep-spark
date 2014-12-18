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

import java.io.Serializable;
import java.util.ArrayList;

public class TextFileDataTable<T> implements Serializable {

    private TableName tableName;

    private String lineSeparator;

    ArrayList<SchemaMap<T>> columnMap;

    public String getLineSeparator() {
        return lineSeparator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    public TextFileDataTable() {
    }

    public TextFileDataTable(TableName tableName, ArrayList<SchemaMap<T>> columnMap) {
        this.tableName = tableName;
        this.columnMap = columnMap;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public ArrayList<SchemaMap<T>> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(ArrayList<SchemaMap<T>> columnMap) {
        this.columnMap = columnMap;
    }
}
