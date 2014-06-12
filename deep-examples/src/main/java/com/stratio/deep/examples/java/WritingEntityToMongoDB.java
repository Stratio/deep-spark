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


        // --- INPUT RDD
        GenericDeepJobConfigMongoDB inputConfig = DeepJobConfigFactory.createMongoDB().host("localhost").port("27017").database("beowulf").collection("input").readPreference("nearest").initialize();

        System.out.println("****************************************************************************");
        JavaRDD inputRDD = deepContext.mongoJavaRDD(inputConfig);

        System.out.println("imprimo el tamaño guay " + inputRDD.count());
        JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, String>() {
            @Override
            public Iterable<String> call(Tuple2<Object, BSONObject> arg) {
                Object o = arg._2().get("text");
                if (o instanceof String) {
                    String str = (String) o;
                    str = str.toLowerCase().replaceAll("[.,!?\n]", " ");
                    System.out.println("imprimo el text " + str);
                    return Arrays.asList(str.split(" "));
                } else {
                    System.out.println("imprimo no es text " );
                    return Collections.emptyList();
                }
            }
        });

        JavaRDD<BSONObject> words2 = inputRDD.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, BSONObject>() {
            @Override
            public Iterable<BSONObject> call(Tuple2<Object, BSONObject> arg) {
                List<BSONObject> lista = new ArrayList<>();
                lista.add(arg._2());
                return lista;
            }
        });
        JavaPairRDD<Object, BSONObject> ones = words2.mapToPair(new PairFunction<BSONObject, Object, BSONObject>() {

            @Override
            public Tuple2<Object, BSONObject> call(BSONObject s) throws Exception {

                return  new Tuple2<>(null, s);
            }
        });



        GenericDeepJobConfigMongoDB inputConfigEntity = DeepJobConfigFactory.createMongoDB(DomainEntity.class).host("localhost").port("27017").database("beowulf").collection("input").readPreference("nearest").initialize();

        JavaRDD inputRDDEntity = deepContext.mongoJavaRDD(inputConfigEntity);

        JavaRDD<BSONObject> entityPrueba = inputRDDEntity.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, BSONObject>() {
            @Override
            public Iterable<BSONObject> call(Tuple2<Object, BSONObject> arg) {
                List<BSONObject> lista = new ArrayList<>();
                lista.add(arg._2());
                return lista;
            }
        });
        JavaPairRDD<Object, DomainEntity> entityPrueba2 = entityPrueba.mapToPair(new PairFunction<BSONObject, Object, DomainEntity>() {

            @Override
            public Tuple2<Object, DomainEntity> call(BSONObject s) throws Exception {

                return  new Tuple2<>(null, UtilMongoDB.getObjectFromBson(DomainEntity.class, s));
            }
        });

//       System.out.println(entityPrueba2.first()._2());

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



        inputRDDEntity = deepContext.mongoJavaRDD(inputConfigEntity);

        MongoRDD.saveRDD((MongoRDD) inputRDDEntity.toRDD(inputRDDEntity), outputConfigEntityPruebaGuardado);
//        MongoEntityRDD.saveEntity(inputRDDEntity, outputConfigEntityPruebaGuardado);


        MongoJavaRDD<DomainEntity> outputRDDEntityPruebaGuardado = deepContext.mongoJavaRDD(outputConfigEntityPruebaGuardado);


        JavaPairRDD<Object, DomainEntity>  pu = MongoRDD.getEntities(outputRDDEntityPruebaGuardado, outputConfigEntityPruebaGuardado);

        List<Tuple2<Object, DomainEntity >> prueba = pu.collect();


        for(Tuple2 tuple : prueba){
            System.out.println("imprimo la segunda parte de la tupla "+tuple._2());
        }




        deepContext.stop();
    }
}
