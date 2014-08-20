package com.stratio.deep.config;

import com.stratio.deep.entity.Cells;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class DeepJobConfig<T> implements Serializable {

    Map<String, String> values;

    Class<IDeepJobConfig<T, ?>> config;

    Class entityClass = Cells.class;

    Class RDDClass;

    Class inputFormatClass;

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }

    public Class<IDeepJobConfig<T, ?>> getConfig() {
        return config;
    }

    public void setConfig(Class<IDeepJobConfig<T, ?>> config) {
        this.config = config;
    }

    public Class getRDDClass() {
        return RDDClass;
    }

    public Class getInputFormatClass() {
        return inputFormatClass;
    }

    public void setInputFormatClass(Class inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
    }

    public void setRDDClass(Class RDDClass) {
        this.RDDClass = RDDClass;

    }


}
