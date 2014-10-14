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

import java.util.List;


import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties;
import com.stratio.deep.mongodb.config.MongoConfigFactory;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.testentity.MessageTestEntity;

import scala.Tuple2;

/**
 * Example class to read an entity from mongoDB
 */
public final class ReadingEntityFromMongoDB {
    private static final Logger LOG = Logger.getLogger(com.stratio.deep.examples.java.ReadingEntityFromMongoDB.class);
    public static List<Tuple2<String, Integer>> results;

    private ReadingEntityFromMongoDB() {
    }


    public static void main(String[] args) {
        doMain(args);
    }


    public static void doMain(String[] args) {
        String job = "java:readingEntityFromMongoDB";

        String host = "localhost:27017";

        String database = "test";
        String inputCollection = "input";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
	    DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());


        MongoDeepJobConfig<MessageTestEntity> inputConfigEntity =
				        MongoConfigFactory.createMongoDB(MessageTestEntity.class).host(host).database(database).collection(inputCollection).initialize();

        JavaRDD<MessageTestEntity> inputRDDEntity = deepContext.createJavaRDD(inputConfigEntity);


	    LOG.info("count : " + inputRDDEntity.cache().count());


        deepContext.stop();
    }
}
