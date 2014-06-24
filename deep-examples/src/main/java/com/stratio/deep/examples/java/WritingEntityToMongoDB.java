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

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.testentity.MessageEntity;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.List;

/**
 * Example class to write an entity to mongoDB
 */
public final class WritingEntityToMongoDB {
    private static final Logger LOG = Logger.getLogger(com.stratio.deep.examples.scala.WritingEntityToMongoDB.class);
    public static List<Tuple2<String, Integer>> results;

    private WritingEntityToMongoDB() {
    }

    /**
     * Application entry point.
     *
     * @param args the arguments passed to the application.
     */
    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:writingEntityToMongoDB";

        String host = "localhost:27017";

        String database = "test";
        String inputCollection = "input";
        String outputCollection = "output";

        String readPreference = "nearest";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());


        IMongoDeepJobConfig<MessageEntity> inputConfigEntity =
                DeepJobConfigFactory.createMongoDB(MessageEntity.class).host(host).database(database).collection(inputCollection).readPreference(readPreference).initialize();

        MongoJavaRDD<MessageEntity> inputRDDEntity = deepContext.mongoJavaRDD(inputConfigEntity);


        IMongoDeepJobConfig<MessageEntity> outputConfigEntityPruebaGuardado =
                DeepJobConfigFactory.createMongoDB(MessageEntity.class).host(host).database(database).collection(outputCollection).readPreference(readPreference).initialize();


        MongoEntityRDD.saveEntity(inputRDDEntity, outputConfigEntityPruebaGuardado);


        deepContext.stop();
    }


}
