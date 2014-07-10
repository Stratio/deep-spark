package com.stratio.deep.utils;

/**
 * Created by rcrespo on 10/07/14.
 */
public enum DeepRDD {


    MONGO_JAVA("com.stratio.deep.rdd.mongodb.MongoJavaRDD"),
    MONGO_ENTITY("com.stratio.deep.rdd.mongodb.MongoEntityRDD"),
    MONGO_CELL("com.stratio.deep.rdd.mongodb.MongoCellRDD"),
    CASSANDRA_JAVA("com.stratio.deep.rdd.CassandraJavaRDD"),
    CASSANDRA_ENTITY("com.stratio.deep.rdd.CassandraEntityRDD"),
    CASSANDRA_CELL("com.stratio.deep.rdd.CassandraCellRDD");

    private String className;

    private DeepRDD(String className) {
        this.className = className;
    }

    public String getClassName(){
        return className;
    }



}
