package com.stratio.deep.commons.querybuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by david on 11/11/14.
 */
public class TableMetadataBuilder {

    private final String catalogName ;
    private final String tableName;
    private final Set<String> primaryKeys;
    private final Set<String> targetFields;

    public TableMetadataBuilder(String catalogName, String tableName) {
        this.catalogName=catalogName;
        this.tableName=tableName;
        primaryKeys= new HashSet<>(1);
        targetFields = new HashSet<>(1);
    }

    public TableMetadataBuilder addPrimaryKey(String... pKeys){
        for(String pKey : pKeys){
            primaryKeys.add(pKey);
        }
        return this;
    }

    public TableMetadataBuilder addTargetFields(String... tFields){
        for(String tField : tFields){
            targetFields.add(tField);
        }
        return this;
    }

    public TableMetadata build(){
        //TODO validate
        return new TableMetadata(catalogName,tableName,primaryKeys,targetFields);
    }
}
