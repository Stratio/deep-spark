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

package com.stratio.deep.examples.java;

import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties;
import com.stratio.deep.mongodb.config.MongoConfigFactory;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;

/**
 * Example class to read a collection from mongoDB
 */
public final class ReadingCellFromMongoDB {
    private static final Logger LOG = Logger.getLogger(ReadingCellFromMongoDB.class);

    private ReadingCellFromMongoDB() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:readingCellFromMongoDB";

        String host = "127.0.0.1:27017";

        String database = "test";
        String inputCollection = "input";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        QueryBuilder query = QueryBuilder.start();
        query.and("number").greaterThan(27).lessThan(30);

        DBObject bsonSort = new BasicDBObject();
        bsonSort.put("number", 1);

        DBObject bsonFields = new BasicDBObject();
        bsonFields.put("number", 1);
        bsonFields.put("text", 1);
        bsonFields.put("_id", 0);

        MongoDeepJobConfig inputConfigEntity = MongoConfigFactory.createMongoDB().host(host).database(database)
                .collection(inputCollection)
                .createInputSplit(false)
                .filterQuery(query)
                .sort(bsonSort).fields(bsonFields);

        RDD inputRDDEntity = deepContext.createRDD(inputConfigEntity);

        LOG.info("count : " + inputRDDEntity.count());
        LOG.info("prints first cell  : " + inputRDDEntity.first());

        deepContext.stop();
    }
}
