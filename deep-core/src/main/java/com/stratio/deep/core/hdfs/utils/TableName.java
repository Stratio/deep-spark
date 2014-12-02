package com.stratio.deep.core.hdfs.utils;

public class TableName {

    private String tableName;

    private String CatalogName;

    public TableName() {
    }

    public TableName(String catalogName , String tableName ) {
        this.tableName = tableName;
        CatalogName = catalogName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return CatalogName;
    }

    public void setCatalogName(String catalogName) {
        CatalogName = catalogName;
    }
}
