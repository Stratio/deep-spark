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
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.rdd.mongodb.*;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 13-feb-2014
 */
public final class WritingEntityFromMongoDBToMongoDB {
    private static final Logger LOG = Logger.getLogger(WritingEntityFromMongoDBToMongoDB.class);
    public static List<Tuple2<String, Integer>> results;

    private WritingEntityFromMongoDBToMongoDB() {
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
        String job = "java:writingEntityToCassandra";

        String keyspaceName = "beowulf";
        String inputTableName = "input";
        String outputTableName = "listdomains";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                new String[]{p.getJar()});



        GenericDeepJobConfigMongoDB inputConfigEntity = DeepJobConfigFactory.createMongoDB(DomainEntity.class).host("localhost").port("27017").database("beowulf").collection("input").readPreference("nearest").initialize();

        JavaRDD inputRDDEntity = deepContext.mongoJavaRDD(inputConfigEntity);


        GenericDeepJobConfigMongoDB outputConfigEntityPruebaGuardado = DeepJobConfigFactory.createMongoDB(DomainEntity.class).host("localhost").port("27017").database("beowulf").collection("output").readPreference("nearest").initialize();



        MongoRDD.saveRDD((MongoRDD) inputRDDEntity.toRDD(inputRDDEntity), outputConfigEntityPruebaGuardado);



        deepContext.stop();
    }


}
