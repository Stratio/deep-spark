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
import com.stratio.deep.config.CellDeepJobConfigMongoDB;
import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.extractor.utils.ExtractorConstants;
import com.stratio.deep.rdd.CassandraCellExtractor;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;

import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.util.HashMap;
import java.util.Map;

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
//        query.and("number").greaterThan(27).lessThan(30);

        BSONObject bsonSort = new BasicBSONObject();
        bsonSort.put("number",1);

        BSONObject bsonFields = new BasicBSONObject();
        bsonFields.put("number",1);
        bsonFields.put("text",1);
        bsonFields.put("_id",0);
        //TODO review

        ExtractorConfig<Cells> config = new ExtractorConfig();

        config.setExtractorImplClass(CellDeepJobConfigMongoDB.class);
        Map<String, String> values = new HashMap<>();
        values.put("database", "test");
        values.put("collection",    "input");
        values.put("host",  "localhost:27017");

        config.setValues(values);



        RDD<Cells> inputRDDEntity = deepContext.createRDD(config);

        LOG.info("****************************************************************************");
        LOG.info("****************************************************************************");
        LOG.info("****************************************************************************");
        LOG.info("****************************************************************************");
        LOG.info("****************************************************************************");
	    LOG.info("****************************************************************************");
        LOG.info("count : " + inputRDDEntity.count());
	    LOG.info("****************************************************************************");
        LOG.info("prints first cell  : " + inputRDDEntity.first());


        deepContext.stop();
    }
}
