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

import com.mongodb.QueryBuilder;
import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.mongodb.MongoCellRDD;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.testentity.MessageEntity;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import scala.Tuple2;

import java.util.List;

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

        BSONObject bsonSort = new BasicBSONObject();
        bsonSort.put("number",1);

        BSONObject bsonFields = new BasicBSONObject();
        bsonFields.put("number",1);
        bsonFields.put("text",1);
        bsonFields.put("_id",0);

        IMongoDeepJobConfig inputConfigEntity = DeepJobConfigFactory.createMongoDB().host(host).database(database)
                .collection(inputCollection)
                .createInputSplit(false)
                .filterQuery(query)
                .sort(bsonSort).fields(bsonFields).initialize();

        MongoCellRDD inputRDDEntity = deepContext.mongoCellRDD(inputConfigEntity);


        System.out.println("count : " + inputRDDEntity.count());


        System.out.println("prints first cell  : " + inputRDDEntity.first());


        deepContext.stop();
    }
}
