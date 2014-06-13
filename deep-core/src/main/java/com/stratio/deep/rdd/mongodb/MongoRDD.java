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

package com.stratio.deep.rdd.mongodb;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.utils.Pair;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.NewHadoopRDD;
import org.bson.BSONObject;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.bson.BasicBSONObject;
import scala.Tuple2;
import java.lang.Iterable;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a {@link com.stratio.deep.entity.Cells}
 * element.
 */
public abstract class MongoRDD<T> extends NewHadoopRDD<Object, BSONObject> {


    private static final long serialVersionUID = -7338324965474684418L;

    /**
     * Transform a row coming from the Cassandra's API to an element of
     * type <T>.
     *
     * @param elem the element to transform.
     * @return the transformed element.
     */
    protected abstract T transformElement(Pair<Object, BSONObject> elem);

    /*
     * RDD configuration. This config is broadcasted to all the Sparks machines.
     *
     */
    protected Broadcast<IDeepJobConfig<T>> config;

    public MongoRDD(SparkContext sc, IDeepJobConfig<T> config) {
        super(sc, MongoInputFormat.class, Object.class, BSONObject.class,
                ((GenericDeepJobConfigMongoDB)config).configHadoop);
    }

    public static <K> void saveRDDPrueba(MongoRDD<K> rdd, IDeepJobConfig<K> writeConfig) {
        GenericDeepJobConfigMongoDB<K> mongoConfig = ((GenericDeepJobConfigMongoDB<K>)writeConfig);
        System.out.println("imrpimo esto antes de |" +mongoConfig.configHadoop.get("mongo.output.uri")+"|");
        
//        rdd.toJavaRDD().fl/
        JavaRDD<String> words = rdd.toJavaRDD().flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, String>() {
            @Override
            public Iterable<String> call(Tuple2<Object, BSONObject> arg) {
                Object o = arg._2().get("text");
                if (o instanceof String) {
                    String str = (String) o;
                    str = str.toLowerCase().replaceAll("[.,!?\n]", " ");
                    return Arrays.asList(str.split(" "));
                } else {
                    return Collections.emptyList();
                }
            }
        });
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }


        });
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaPairRDD<Object, BSONObject> save = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Object, BSONObject>() {
            @Override
            public Tuple2<Object, BSONObject> call(Tuple2<String, Integer> tuple) {
                BSONObject bson = new BasicBSONObject();
                bson.put("word", tuple._1());
                bson.put("count", tuple._2());
//                System.out.println(" imprimo lo que voy a guardar jojojo " + tuple._1 + " las veces que se repite : "+ tuple._2);
                return new Tuple2<>(null, bson);
            }
        });
        save.saveAsNewAPIHadoopFile("file:///bogus", Object.class, Object.class, MongoOutputFormat.class, mongoConfig.configHadoop);
    }

    public static <T> void saveRDD(MongoRDD<T> rdd, GenericDeepJobConfigMongoDB<T> writeConfig) {
        JavaRDD<BSONObject> words = rdd.toJavaRDD().flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, BSONObject>() {
            @Override
            public Iterable<BSONObject> call(Tuple2<Object, BSONObject> arg) {
                List<BSONObject> lista = new ArrayList<>();
                lista.add(arg._2());
                return lista;
            }
        });
        JavaPairRDD<Object, BSONObject> ones = words.mapToPair(new PairFunction<BSONObject, Object, BSONObject>() {

            @Override
            public Tuple2<Object, BSONObject> call(BSONObject s) throws Exception {

                return  new Tuple2<>(null, s);
            }
        });

        ones.saveAsNewAPIHadoopFile( "file:///bogus", Object.class, BSONObject.class, MongoOutputFormat.class, writeConfig.configHadoop);



    }


    public static <Object,V extends IDeepType> JavaPairRDD<Object,V> getEntities(MongoRDD<V> rdd, final GenericDeepJobConfigMongoDB<V> genericDeepJobConfigMongoDB){

        JavaRDD<BSONObject> javaRDD = rdd.toJavaRDD().flatMap(new FlatMapFunction<Tuple2<java.lang.Object, BSONObject>, BSONObject>() {
            @Override
            public Iterable<BSONObject> call(Tuple2<java.lang.Object, BSONObject> arg) {
                List<BSONObject> lista = new ArrayList<>();
                lista.add(arg._2());
                return lista;
            }
        });
        JavaPairRDD<Object, V> entityPar= javaRDD.mapToPair(new  PairFunction<BSONObject, Object, V>() {

            @Override
            public Tuple2<Object, V> call(BSONObject s) throws Exception {


                return  new Tuple2<>(null, UtilMongoDB.getObjectFromBson(genericDeepJobConfigMongoDB.getEntityClass(), s));
            }

        });

        return entityPar;
    }

    public static <Object,V extends IDeepType> JavaPairRDD<Object,V> getEntities(MongoJavaRDD<V> javaRDD, final GenericDeepJobConfigMongoDB<V> genericDeepJobConfigMongoDB){

        return getEntities( (MongoRDD) javaRDD.toRDD(javaRDD), genericDeepJobConfigMongoDB);
    }

    @Override
    public InterruptibleIterator<Tuple2<Object, BSONObject>> compute(Partition theSplit, TaskContext context) {
        return super.compute(theSplit, context);
    }
}
