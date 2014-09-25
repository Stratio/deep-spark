package com.stratio.deep.commons.extractor.utils;

import org.apache.cassandra.db.ConsistencyLevel;

/**
 * Created by dgomez on 25/08/14.
 */
public class ExtractorConstants {

    public static  String PASSWORD = "password";


    public static  String CATALOG = "catalog";
    public static  String KEYSPACE = CATALOG;
    public static  String DATABASE = KEYSPACE;
    public static  String INDEX = CATALOG;
    public static  String TABLE    = "table";
    public static  String COLLECTION = TABLE;
    public static  String TYPE = TABLE;
    public static  String HOST     = "host";
    public static  String PORT     = "port";
    public static  String INPUT_COLUMNS = "inputColumns";
    public static  String USERNAME  ="user";
    public static  String PAGE_SIZE = "page";
    public static  String SESSION  = "session";
    public static  String RPCPORT  = "rpcPort";
    public static  String CQLPORT  = "cqlPort";
    public static  String COLUMN_FAMILY  = "columnFamily";
    public static  String BISECT_FACTOR  = "bisecFactor";
    public static  String CREATE_ON_WRITE  = "createOnWrite";
    public static  String BATCHSIZE  = "batchSize";
    public static  String READ_CONSISTENCY_LEVEL = "readConsistencyLevel";
    public static String WRITE_CONSISTENCY_LEVEL = "writeConsistencyLevel";

    public static String REPLICA_SET = "replicaSet";

    public static String READ_PREFERENCE = "readPreference";

    public static String SORT = "sort";

    public static String FILTER_FIELD = "filterField";


    public static String FILTER_QUERY = "filterQuery";

    public static String INPUT_KEY = "inputKey";

    public static String IGNORE_ID_FIELD = "ignoreIdField";

    public static String USE_SHARD = "useShard";

    public static String USE_SPLITS = "useSplit";

    public static String USE_CHUNKS = "useChunk";

    public static String SPLIT_SIZE = "splitSize";

}
