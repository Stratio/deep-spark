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

package com.stratio.deep.rdd;

import com.stratio.deep.rdd.*;
import com.stratio.deep.config.IESDeepJobConfig;
import com.stratio.deep.config.ESConfigFactory;
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
import org.testng.annotations.Test;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.testng.Assert.*;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESEntityRDDTest"})
public class ESEntityRDDTest implements Serializable {
//
// @Test
// public void testReadingRDD() {
// String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<MessageTestEntity> inputConfigEntity = ESConfigFactory.createES(MessageTestEntity.class)
// .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();
//
// JavaRDD<MessageTestEntity> inputRDDEntity = context.ESJavaRDD(inputConfigEntity);
//
// assertEquals(ESJavaRDDTest.col.count(), inputRDDEntity.cache().count());
// assertEquals(ESJavaRDDTest.col.findOne().get("message"), inputRDDEntity.first().getMessage());
//
// context.stop();
//
// }
//
// @Test
// public void testWritingRDD() {
//
//
// String hostConcat = HOST.concat(":").concat(PORT.toString());
//
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<MessageTestEntity> inputConfigEntity = ESConfigFactory.createES(MessageTestEntity.class)
// .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();
//
// RDD<MessageTestEntity> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
// IESDeepJobConfig<MessageTestEntity> outputConfigEntity = ESConfigFactory.createES(MessageTestEntity.class)
// .host(hostConcat).database(DATABASE).collection(COLLECTION_OUTPUT).initialize();
//
//
// //Save RDD in ES
// ESEntityRDD.saveEntity(inputRDDEntity, outputConfigEntity);
//
// RDD<MessageTestEntity> outputRDDEntity = context.ESRDD(outputConfigEntity);
//
//
// assertEquals(ESJavaRDDTest.ES.getDB(DATABASE).getCollection(COLLECTION_OUTPUT).findOne().get("message"),
// outputRDDEntity.first().getMessage());
//
//
// context.stop();
//
//
// }
//
//
// @Test
// public void testDataSet() {
//
// String hostConcat = HOST.concat(":").concat(PORT.toString());
//
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<BookEntity> inputConfigEntity = ESConfigFactory.createES(BookEntity.class)
// .host(hostConcat).database("book").collection("input")
// .initialize();
//
// RDD<BookEntity> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
//
////Import dataSet was OK and we could read it
// assertEquals(1, inputRDDEntity.count());
//
//
// List<BookEntity> books = inputRDDEntity.toJavaRDD().collect();
//
//
// BookEntity book = books.get(0);
//
// DBObject proObject = new BasicDBObject();
// proObject.put("_id", 0);
//
//
//// tests subDocuments
// BSONObject result = ESJavaRDDTest.ES.getDB("book").getCollection("input").findOne(null, proObject);
// assertEquals(((DBObject) result.get("metadata")).get("author"), book.getMetadataEntity().getAuthor());
//
//
//// tests List<subDocuments>
// List<BSONObject> listCantos = (List<BSONObject>) result.get("cantos");
// BSONObject bsonObject = null;
//
// for (int i = 0; i < listCantos.size(); i++) {
// bsonObject = listCantos.get(i);
// assertEquals(bsonObject.get("canto"), book.getCantoEntities().get(i).getNumber());
// assertEquals(bsonObject.get("text"), book.getCantoEntities().get(i).getText());
// }
//
// RDD<BookEntity> inputRDDEntity2 = context.ESRDD(inputConfigEntity);
//
// JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<BookEntity, String>() {
// @Override
// public Iterable<String> call(BookEntity bookEntity) throws Exception {
//
// List<String> words = new ArrayList<>();
// for (CantoEntity canto : bookEntity.getCantoEntities()) {
// words.addAll(Arrays.asList(canto.getText().split(" ")));
// }
// return words;
// }
// });
//
//
// JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
// @Override
// public Tuple2<String, Integer> call(String s) throws Exception {
// return new Tuple2<String, Integer>(s, 1);
// }
// });
//
//
// JavaPairRDD<String, Integer> wordCountReduced = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
// @Override
// public Integer call(Integer integer, Integer integer2) throws Exception {
// return integer + integer2;
// }
// });
//
// JavaRDD<WordCount> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Integer>, WordCount>() {
// @Override
// public WordCount call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
// return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
// }
// });
//
//
// IESDeepJobConfig<WordCount> outputConfigEntity =
// ESConfigFactory.createES(WordCount.class).host(hostConcat).database("book").collection("output")
// .initialize();
//
// ESEntityRDD.saveEntity((RDD<WordCount>) outputRDD.rdd(), outputConfigEntity);
//
// RDD<WordCount> outputRDDEntity = context.ESRDD(outputConfigEntity);
//
// assertEquals(((Long) outputRDDEntity.cache().count()).longValue(), WORD_COUNT_SPECTED.longValue());
//
// context.stop();
//
// }
//
//
// @Test
// public void testInputColums() {
//
// String hostConcat = HOST.concat(":").concat(PORT.toString());
//
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<BookEntity> inputConfigEntity = ESConfigFactory.createES(BookEntity.class)
// .host(hostConcat).database("book").collection("input").inputColumns("metadata").initialize();
//
// RDD<BookEntity> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
//
// BookEntity bookEntity = inputRDDEntity.toJavaRDD().first();
//
//
// assertNotNull(bookEntity.getId());
// assertNotNull(bookEntity.getMetadataEntity());
// assertNull(bookEntity.getCantoEntities());
//
//
// IESDeepJobConfig<BookEntity> inputConfigEntity2 = ESConfigFactory.createES(BookEntity.class)
// .host(hostConcat).database("book").collection("input").inputColumns("cantos").ignoreIdField().initialize();
//
//
// RDD<BookEntity> inputRDDEntity2 = context.ESRDD(inputConfigEntity2);
//
//
// BookEntity bookEntity2 = inputRDDEntity2.toJavaRDD().first();
//
// assertNull(bookEntity2.getId());
// assertNull(bookEntity2.getMetadataEntity());
// assertNotNull(bookEntity2.getCantoEntities());
//
//
// BSONObject bson = new BasicBSONObject();
//
// bson.put("_id", 0);
// bson.put("cantos", 1);
//
// IESDeepJobConfig<BookEntity> inputConfigEntity3 = ESConfigFactory.createES(BookEntity.class)
// .host(hostConcat).database("book").collection("input").fields(bson).initialize();
//
// RDD<BookEntity> inputRDDEntity3 = context.ESRDD(inputConfigEntity3);
//
//
// BookEntity bookEntity3 = inputRDDEntity3.toJavaRDD().first();
//
// assertNull(bookEntity3.getId());
// assertNull(bookEntity3.getMetadataEntity());
// assertNotNull(bookEntity3.getCantoEntities());
//
// }
}