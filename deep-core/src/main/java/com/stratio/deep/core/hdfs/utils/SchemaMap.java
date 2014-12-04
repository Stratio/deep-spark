package com.stratio.deep.core.hdfs.utils;

import java.io.Serializable;

public class SchemaMap<T> implements Serializable {

    private String fieldName;

    private Class<T> fieldType;

    public SchemaMap(String fieldName, Class<T> fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Class<T> getFieldType() {
        return fieldType;
    }

    public void setFieldType(Class<T> fieldType) {
        this.fieldType = fieldType;
    }
}
