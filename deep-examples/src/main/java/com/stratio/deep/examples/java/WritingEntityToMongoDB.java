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
import com.stratio.deep.rdd.mongodb.DomainEntity;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.rdd.mongodb.UtilMongoDB;
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


public final class WritingEntityToMongoDB {
    private static final Logger LOG = Logger.getLogger(WritingEntityToMongoDB.class);
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

    /**
     * This is the method called by both main and tests.
     *
     * @param args
     */
    /**
     * This is the method called by both main and tests.
     *
     * @param args
     */
    public static void doMain(String[] args) {
        String job = "java:writingEntityToCassandra";

        String keyspaceName = "beowulf";
        String inputTableName = "input";
        String outputTableName = "listdomains";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                new String[]{p.getJar()});


        DomainEntity domainEntity1 = new DomainEntity();

        domainEntity1.setId(new ObjectId());
        domainEntity1.setText("Texto1");

        DomainEntity domainEntity2 = new DomainEntity();

        domainEntity2.setId(new ObjectId());
        domainEntity2.setText("Texto1");


        List<DomainEntity> entities = new ArrayList<>();
        entities.add(domainEntity1);
        entities.add(domainEntity2);



        GenericDeepJobConfigMongoDB outputConfigEntityPruebaGuardado = DeepJobConfigFactory.createMongoDB(DomainEntity.class).host("localhost").port("27017").database("beowulf").collection("output.generated").readPreference("nearest").initialize();


        MongoJavaRDD<DomainEntity> outputRDDEntity = deepContext.mongoJavaRDD(outputConfigEntityPruebaGuardado);



        MongoEntityRDD.saveEntity(outputRDDEntity, outputConfigEntityPruebaGuardado);




        deepContext.stop();
    }
}
