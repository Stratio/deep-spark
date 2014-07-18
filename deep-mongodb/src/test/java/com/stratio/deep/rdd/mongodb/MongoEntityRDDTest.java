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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.config.MongoConfigFactory;
import com.stratio.deep.context.MongoDeepSparkContext;
import com.stratio.deep.testentity.BookEntity;
import com.stratio.deep.testentity.CantoEntity;
import com.stratio.deep.testentity.MessageTestEntity;
import com.stratio.deep.testentity.WordCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.stratio.deep.rdd.mongodb.MongoJavaRDDTest.*;
import static org.testng.Assert.*;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test(suiteName = "mongoRddTests", groups = {"MongoEntityRDDTest"})
public class MongoEntityRDDTest implements Serializable {


    @Test
    public void testReadingRDD() {
        String hostConcat = MongoJavaRDDTest.HOST.concat(":").concat(MongoJavaRDDTest.PORT.toString());
        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<MessageTestEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(MessageTestEntity.class)
                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();

        JavaRDD<MessageTestEntity> inputRDDEntity = context.mongoJavaRDD(inputConfigEntity);

        assertEquals(MongoJavaRDDTest.col.count(), inputRDDEntity.cache().count());
        assertEquals(MongoJavaRDDTest.col.findOne().get("message"), inputRDDEntity.first().getMessage());

        context.stop();

    }

    @Test
    public void testWritingRDD() {


        String hostConcat = HOST.concat(":").concat(PORT.toString());

        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<MessageTestEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(MessageTestEntity.class)
                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();

        RDD<MessageTestEntity> inputRDDEntity = context.mongoRDD(inputConfigEntity);

        IMongoDeepJobConfig<MessageTestEntity> outputConfigEntity = MongoConfigFactory.createMongoDB(MessageTestEntity.class)
                .host(hostConcat).database(DATABASE).collection(COLLECTION_OUTPUT).initialize();


        //Save RDD in MongoDB
        MongoEntityRDD.saveEntity(inputRDDEntity, outputConfigEntity);

        RDD<MessageTestEntity> outputRDDEntity = context.mongoRDD(outputConfigEntity);


        assertEquals(MongoJavaRDDTest.mongo.getDB(DATABASE).getCollection(COLLECTION_OUTPUT).findOne().get("message"),
                outputRDDEntity.first().getMessage());


        context.stop();


    }


    @Test
    public void testDataSet() {

        String hostConcat = HOST.concat(":").concat(PORT.toString());

        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<BookEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(BookEntity.class)
                .host(hostConcat).database("book").collection("input")
                .initialize();

        RDD<BookEntity> inputRDDEntity = context.mongoRDD(inputConfigEntity);


//Import dataSet was OK and we could read it
        assertEquals(1, inputRDDEntity.count());


        List<BookEntity> books = inputRDDEntity.toJavaRDD().collect();


        BookEntity book = books.get(0);

        DBObject proObject = new BasicDBObject();
        proObject.put("_id", 0);


//      tests subDocuments
        BSONObject result = MongoJavaRDDTest.mongo.getDB("book").getCollection("input").findOne(null, proObject);
        assertEquals(((DBObject) result.get("metadata")).get("author"), book.getMetadataEntity().getAuthor());


//      tests List<subDocuments>
        List<BSONObject> listCantos = (List<BSONObject>) result.get("cantos");
        BSONObject bsonObject = null;

        for (int i = 0; i < listCantos.size(); i++) {
            bsonObject = listCantos.get(i);
            assertEquals(bsonObject.get("canto"), book.getCantoEntities().get(i).getNumber());
            assertEquals(bsonObject.get("text"), book.getCantoEntities().get(i).getText());
        }

        RDD<BookEntity> inputRDDEntity2 = context.mongoRDD(inputConfigEntity);

        JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<BookEntity, String>() {
            @Override
            public Iterable<String> call(BookEntity bookEntity) throws Exception {

                List<String> words = new ArrayList<>();
                for (CantoEntity canto : bookEntity.getCantoEntities()) {
                    words.addAll(Arrays.asList(canto.getText().split(" ")));
                }
                return words;
            }
        });


        JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });


        JavaPairRDD<String, Integer> wordCountReduced = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<WordCount> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Integer>, WordCount>() {
            @Override
            public WordCount call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
            }
        });


        IMongoDeepJobConfig<WordCount> outputConfigEntity =
                MongoConfigFactory.createMongoDB(WordCount.class).host(hostConcat).database("book").collection("output")
                        .initialize();

        MongoEntityRDD.saveEntity((RDD<WordCount>) outputRDD.rdd(), outputConfigEntity);

        RDD<WordCount> outputRDDEntity = context.mongoRDD(outputConfigEntity);

        assertEquals(((Long) outputRDDEntity.cache().count()).longValue(), WORD_COUNT_SPECTED.longValue());

        context.stop();

    }


    @Test
    public void testInputColums() {

        String hostConcat = HOST.concat(":").concat(PORT.toString());

        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<BookEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(BookEntity.class)
                .host(hostConcat).database("book").collection("input").inputColumns("metadata").initialize();

        RDD<BookEntity> inputRDDEntity = context.mongoRDD(inputConfigEntity);


        BookEntity bookEntity = inputRDDEntity.toJavaRDD().first();


        assertNotNull(bookEntity.getId());
        assertNotNull(bookEntity.getMetadataEntity());
        assertNull(bookEntity.getCantoEntities());


        IMongoDeepJobConfig<BookEntity> inputConfigEntity2 = MongoConfigFactory.createMongoDB(BookEntity.class)
                .host(hostConcat).database("book").collection("input").inputColumns("cantos").ignoreIdField().initialize();


        RDD<BookEntity> inputRDDEntity2 = context.mongoRDD(inputConfigEntity2);


        BookEntity bookEntity2 = inputRDDEntity2.toJavaRDD().first();

        assertNull(bookEntity2.getId());
        assertNull(bookEntity2.getMetadataEntity());
        assertNotNull(bookEntity2.getCantoEntities());


        BSONObject bson = new BasicBSONObject();

        bson.put("_id", 0);
        bson.put("cantos", 1);

        IMongoDeepJobConfig<BookEntity> inputConfigEntity3 = MongoConfigFactory.createMongoDB(BookEntity.class)
                .host(hostConcat).database("book").collection("input").fields(bson).initialize();

        RDD<BookEntity> inputRDDEntity3 = context.mongoRDD(inputConfigEntity3);


        BookEntity bookEntity3 = inputRDDEntity3.toJavaRDD().first();

        assertNull(bookEntity3.getId());
        assertNull(bookEntity3.getMetadataEntity());
        assertNotNull(bookEntity3.getCantoEntities());

    }


}
