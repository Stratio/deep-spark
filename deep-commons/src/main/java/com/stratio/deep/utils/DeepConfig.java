package com.stratio.deep.utils;

/**
 * Created by rcrespo on 10/07/14.
 */
public enum DeepConfig {

    MONGODB_CELL("com.stratio.deep.config.CellDeepJobConfigMongoDB"),
    MONGODB_ENTITY("com.stratio.deep.config.EntityDeepJobConfigMongoDB"),
    CASSANDRA_CELL("com.stratio.deep.config.CellDeepJobConfig"),
    CASSANDRA_ENTITY("com.stratio.deep.config.EntityDeepJobConfig");

    private String config;

    private DeepConfig(String config) {
        this.config = config;
    }

    public String getConfig(){
        return config;
    }
}
