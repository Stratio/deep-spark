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
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.rdd.mongodb.MongoRDD;
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


public final class ReadingEntityFromMongoDB {
    private static final Logger LOG = Logger.getLogger(ReadingEntityFromMongoDB.class);
    public static List<Tuple2<String, Integer>> results;

    private ReadingEntityFromMongoDB() {
    }


    public static void main(String[] args) {
        doMain(args);
    }


    public static void doMain(String[] args) {
        String job = "java:readingEntityFromCassandra";


        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                new String[]{p.getJar()});



        GenericDeepJobConfigMongoDB inputConfigEntity = DeepJobConfigFactory.createMongoDB(DomainEntity.class).host("localhost").port("27017").database("beowulf").collection("input").readPreference("nearest").initialize();

        MongoJavaRDD<DomainEntity> inputRDDEntity = deepContext.mongoJavaRDD(inputConfigEntity);



        JavaPairRDD<Object, DomainEntity>  pair = MongoRDD.getEntities(inputRDDEntity, inputConfigEntity);

        List<Tuple2<Object, DomainEntity >> entities = pair.collect();


        for(Tuple2 tuple : entities){
            System.out.println(tuple._2());
        }




        deepContext.stop();
    }
}
