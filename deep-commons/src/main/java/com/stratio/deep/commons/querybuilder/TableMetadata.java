package com.stratio.deep.commons.querybuilder;

import java.util.Set;

/**
 * Created by david on 11/11/14.
 */
public class TableMetadata {
    private final String catalogName;
    private final String tableName;
    private final Set<String> primaryKeys;
    private final Set<String> targetFields;

    public TableMetadata(String catalogName, String tableName, Set<String> primaryKeys, Set<String> targetFields){
        this.catalogName=catalogName;
        this.tableName=tableName;
        this.primaryKeys=primaryKeys;
        this.targetFields=targetFields;
    }

    public static TableMetadataBuilder createTableMetadata(String catalogName, String tableName){
        return new TableMetadataBuilder(catalogName, tableName);
    }


}
