package com.stratio.deep.commons.extractor.utils;

/**
 * Created by dgomez on 25/08/14.
 */
public interface ExtractorConstants {

    /**
     * The PASSWORD.
     */
    String PASSWORD = "password";

    /**
     * The CATALOG.
     */
    String CATALOG = "catalog";
    /**
     * The KEYSPACE.
     */
    String KEYSPACE = CATALOG;
    /**
     * The DATABASE.
     */
    String DATABASE = KEYSPACE;
    /**
     * The INDEX.
     */
    String INDEX = CATALOG;
    /**
     * The TABLE.
     */
    String TABLE = "table";
    /**
     * The COLLECTION.
     */
    String COLLECTION = TABLE;
    /**
     * The TYPE.
     */
    String TYPE = TABLE;
    /**
     * The NAMESPACE.
     */
    String NAMESPACE = CATALOG;
    /**
     * The SET.
     */
    String SET = TABLE;
    /**
     * The HOST.
     */
    String HOST = "host";
    /**
     * The PORT.
     */
    String PORT = "Port";
    /**
     * The INPUT _ cOLUMNS.
     */
    String INPUT_COLUMNS = "inputColumns";
    /**
     * The USERNAME.
     */
    String USERNAME = "user";
    /**
     * The PAGE _ sIZE.
     */
    String PAGE_SIZE = "page";
    /**
     * The SESSION.
     */
    String SESSION = "session";
    /**
     * JDBC driver class name.
     */
    String JDBC_DRIVER_CLASS = "driverClass";
    /**
     * JDBC connection URL.
     */
    String JDBC_CONNECTION_URL = "connectionUrl";
    /**
     * JDBC query.
     */
    String JDBC_QUERY = "jdbcQuery";
    /**
     * Quoted SQL table names and fields.
     */
    String JDBC_QUOTE_SQL = "jdbcQuoteSQL";
    /**
     * Column name use for JDBC partitioning.
     */
    String JDBC_PARTITION_KEY = "partitionKey";
    /**
     * Jdbc partitions lower bound
     */
    String JDBC_PARTITIONS_LOWER_BOUND = "lowerBound";
    /**
     * Jdbc partitions upper bound
     */
    String JDBC_PARTITIONS_UPPER_BOUND = "upperBound";
    /**
     * Jdbc number of partitions
     */
    String JDBC_NUM_PARTITIONS = "numPartitions";
    /**
     * The PORT 2.
     */
    String PORT2 = "port2";
    /**
     * The CQLPORT.
     */
    String CQLPORT = PORT;
    /**
     * The RPCPORT.
     */
    String RPCPORT = PORT2;

    /**
     * The RES_PORT.for ElasticSearch
     */
    String ES_REST_PORTS = "Restful Ports";;
    /**
     * The COLUMN _ fAMILY.
     */
    String COLUMN_FAMILY = "columnFamily";
    /**
     * The BISECT _ fACTOR.
     */
    String BISECT_FACTOR = "bisecFactor";
    /**
     * The CREATE _ ON _ WRITE.
     */
    String CREATE_ON_WRITE = "createOnWrite";
    /**
     * The BATCHSIZE.
     */
    String BATCHSIZE = "batchSize";
    /**
     * The READ _ CONSISTENCY _ lEVEL.
     */
    String READ_CONSISTENCY_LEVEL = "readConsistencyLevel";
    /**
     * The WRITE _ CONSISTENCY _ lEVEL.
     */
    String WRITE_CONSISTENCY_LEVEL = "writeConsistencyLevel";

    /**
     * The REPLICA _ SET.
     */
    String REPLICA_SET = "replicaSet";

    /**
     * The READ _ PREFERENCE.
     */
    String READ_PREFERENCE = "readPreference";

    /**
     * The SORT.
     */
    String SORT = "sort";

    /**
     * The FILTER _ FIELD.
     */
    String FILTER_FIELD = "filterField";

    /**
     * The FILTER _ QUERY.
     */
    String FILTER_QUERY = "filterQuery";

    /**
     * The INPUT _ kEY.
     */
    String INPUT_KEY = "inputKey";

    /**
     * The IGNORE _ ID _ FIELD.
     */
    String IGNORE_ID_FIELD = "ignoreIdField";

    /**
     * The USE _ sHARD.
     */
    String USE_SHARD = "useShard";

    /**
     * The USE _ SPLITS.
     */
    String USE_SPLITS = "useSplit";

    /**
     * The USE _ CHUNKS.
     */
    String USE_CHUNKS = "useChunk";

    /**
     * The SPLIT _ SIZE.
     */
    String SPLIT_SIZE = "splitSize";

    /**
     * The EQUALS _ IN _ FILTER.
     */
    String EQUALS_IN_FILTER = "equalsInFilter";

    /**
     * The HOSTS.
     */
    String HOSTS    = "Hosts";

    /**
     * The PORTS.
     */
    String PORTS    = "Port";

    /**
     * The HDFS.
     */
    String HDFS       = "hdfs";
    /**
     * The HDFS _ tYPE.
     */
    String HDFS_TYPE  = "hdfsType";

    /**
     * The FS prefix
     */
    String FS_PREFIX = "prefix";

    /**
     * The FS _ FileDataTable.
     */
    String FS_FILEDATATABLE = "TextFileDataTable";
    /**
     * The FS file Separator.
     */
    String FS_FILE_SEPARATOR = "FileSeparator";
    /**
     * The HDFS MAP.
     */
    String HDFS_MAP     = "map";
    /**
     * The HDFS Prefix
     */
    String HDFS_PREFIX = "hdfs://";
    /**
     * The HDFS file path.
     */
    String FS_FILE_PATH = "path";
    /**
     * The FS schema
     */
    String FS_SCHEMA = "schemaMap";
    /**
     * The HDFS file extension
     */
    String HDFS_FILE_EXTENSION = "Extension";

    /**
     * S3
     */
    String S3 = "s3";

    /**
     * S3 Prefix
     */
    String S3_PREFIX = "s3n://";

    /**
     * S3 type
     */
    String S3_TYPE = "s3Type";

    /**
     * S3 Bucket
     */
    String S3_BUCKET = "bucket";

    /**
     * S3 AWS Access Key ID
     */
    String S3_ACCESS_KEY_ID = "s3Key";

    /**
     * S3 Secret Access Key
     */
    String S3_SECRET_ACCESS_KEY = "s3Secret";

    /**
     * S3 File path
     */
    String S3_FILE_PATH = "path";


    /**
     * The TYPE
     */
    String TYPE_CONSTANT = "type";
    /**
     * The INNERCLASS.
     */
    String INNERCLASS  ="implClass";


    String WRITE_MODE = "writeMode";
}
