/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.extractor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.extractor.ExtractorTest;
import com.stratio.deep.mongodb.extractor.MongoEntityExtractor;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.CantoEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.entity.WordCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import scala.Tuple2;

import static com.stratio.deep.extractor.MongoJavaRDDTest.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test(suiteName = "mongoRddTests", groups = {"MongoEntityExtractorTest"} , dependsOnGroups = "MongoJavaRDDTest")
public class MongoEntityExtractorTest extends ExtractorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MongoEntityExtractorTest.class);

    public MongoEntityExtractorTest() {
        super(MongoEntityExtractor.class, "localhost:27890", null, false);
    }


    @Test
    public void testDataSet() {

        String hostConcat = HOST.concat(":").concat(PORT.toString());

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");


        ExtractorConfig<BookEntity> inputConfigEntity = new ExtractorConfig(BookEntity.class);
        inputConfigEntity.putValue(ExtractorConstants.HOST,hostConcat).putValue(ExtractorConstants.DATABASE, "book").putValue(ExtractorConstants.COLLECTION,"input");
        inputConfigEntity.setExtractorImplClass(MongoEntityExtractor.class);


        RDD<BookEntity> inputRDDEntity = context.createRDD(inputConfigEntity);


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

        for (int i = 0; i < listCantos.size(); i++) {
            BSONObject bsonObject = listCantos.get(i);
            assertEquals(bsonObject.get("canto"), book.getCantoEntities().get(i).getNumber());
            assertEquals(bsonObject.get("text"), book.getCantoEntities().get(i).getText());
        }

        RDD<BookEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity);

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


        ExtractorConfig<WordCount> outputConfigEntity = new ExtractorConfig(WordCount.class);
        outputConfigEntity.putValue(ExtractorConstants.HOST,hostConcat).putValue(ExtractorConstants.DATABASE, "book").putValue(ExtractorConstants.COLLECTION,"output");
        outputConfigEntity.setExtractorImplClass(MongoEntityExtractor.class);


        context.saveRDD(outputRDD.rdd(), outputConfigEntity);

        RDD<WordCount> outputRDDEntity = context.createRDD(outputConfigEntity);

        assertEquals(((Long) outputRDDEntity.cache().count()).longValue(), WORD_COUNT_SPECTED.longValue());

        context.stop();

    }



}
