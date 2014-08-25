package com.stratio.deep.config;


import com.stratio.deep.entity.Cells;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class ExtractorConfig<T> implements Serializable {

    private Map<String, String> values;

    private Class ExtractorImplClass;


    private Class entityClass = Cells.class;


    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }

    public Class getExtractorImplClass() {
        return ExtractorImplClass;
    }

    public void setExtractorImplClass(Class extractorImplClass) {
        ExtractorImplClass = extractorImplClass;
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public void putValue(String key, String value){
        values.put(key, value);
    }
}
