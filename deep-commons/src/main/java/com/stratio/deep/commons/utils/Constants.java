/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.commons.utils;

/**
 * Class holding several public Deep constants
 * 
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class Constants {

    public static final String DEFAULT_CASSANDRA_HOST = "localhost";
    public static final int DEFAULT_CASSANDRA_RPC_PORT = 9160;
    public static final int DEFAULT_CASSANDRA_CQL_PORT = 9042;

    public static final String DEFAULT_MONGO_HOST = "localhost:27017";

    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final int DEFAULT_PAGE_SIZE = 1000;
    public static final int DEFAULT_MAX_PAGE_SIZE = 10000;

    public static final int DEFAULT_BISECT_FACTOR = 1;

    public static final String SPARK_PARTITION_ID = "spark.partition.id";

    public static final String SPARK_RDD_ID = "spark.rdd.id";

    private Constants() {
    }

}
