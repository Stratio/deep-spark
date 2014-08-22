package com.stratio.deep.config;


import com.stratio.deep.entity.Cells;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class ExtractorConfig<T> implements Serializable {

    private Map<String, String> values;

    private Class RDDClass;

    private Class inputFormatClass;

    private Class entityClass;

    public ExtractorConfig (Class t){
        super();
        entityClass = t;
    }

    public ExtractorConfig(){
        entityClass = Cells.class;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }

    public Class getRDDClass() {
        return RDDClass;
    }

    public void setRDDClass(Class RDDClass) {
        this.RDDClass = RDDClass;
    }

    public Class getInputFormatClass() {
        return inputFormatClass;
    }

    public void setInputFormatClass(Class inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
    }


    public Class getEntityClass() {
        return entityClass;
    }

//    public void setEntityClass(Class entityClass) {
//        this.entityClass = entityClass;
//    }
}

