package com.stratio.deep.core.hdfs.utils;

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
