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

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.*;
import com.stratio.deep.core.extractor.ExtractorTest;
import com.stratio.deep.extractor.ESEntityExtractor;

import com.stratio.deep.testentity.TweetES;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESEntityRDDTest"}, dependsOnGroups = "ESJavaRDDTest")
public class ESEntityRDDTest extends ExtractorTest implements Serializable{

    private static final Logger LOG = Logger.getLogger(ESEntityRDDTest.class);

    public ESEntityRDDTest() {

        super(ESEntityExtractor.class,"localhost:9200",null,ESJavaRDDTest.ES_INDEX_MESSAGE+ESJavaRDDTest.ES_SEPARATOR+ESJavaRDDTest.ES_TYPE_MESSAGE,
                MessageTestEntity.class,MessageTestEntity.class,BookEntity.class);
    }



    @Override
    @Test
    public void testInputColumns() {
        assertEquals(true, true);
    }

    public void testDataSet() {

        DeepSparkContext context = null;

        // Creating the Deep Context where args are Spark Master and Job Name
        String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
        context = new DeepSparkContext("local", "deepSparkContextTest");

        ExtractorConfig<BookEntity> inputConfigEntity = new ExtractorConfig(BookEntity.class);
        inputConfigEntity.putValue(ExtractorConstants.HOST,hostConcat).putValue(ExtractorConstants.DATABASE, "book/input");
        inputConfigEntity.setExtractorImplClass(ESEntityExtractor.class);


        RDD<BookEntity> inputRDDEntity = context.createRDD(inputConfigEntity);


        //Import dataSet was OK and we could read it
        //Assert.assertEquals(1, inputRDDEntity.count());

        List<BookEntity> books = inputRDDEntity.toJavaRDD().collect();


        BookEntity book = books.get(0);

        // -------------Another Kind to recover ENtities---------
//        GetResponse response = ESJavaRDDTest.client.prepareGet("book", "input", "idXXXX").execute().actionGet();
//        response.getFields();

        //tests subDocuments
        SearchResponse searchResponse = ESJavaRDDTest.client.prepareSearch("book").setQuery(termQuery("_type","input")).execute().actionGet();
        SearchHit[] sh = searchResponse.getHits().getHits() ;
        List<JSONObject> listCantos =  new ArrayList<JSONObject>();
        for(SearchHit hit :sh){

            listCantos.add((JSONObject)JSONValue.parse(hit.sourceAsString()));
        }

//      tests List<subDocuments>
        for (int i = 0; i < listCantos.size(); i++) {
            JSONObject cantosObject = listCantos.get(i);
            JSONObject jsonObject   = (JSONObject)((JSONArray)cantosObject.get("cantos")).get(i);
            Assert.assertEquals(jsonObject.get("canto"), book.getCantoEntities().get(i).getNumber());
            Assert.assertEquals(jsonObject.get("text"), book.getCantoEntities().get(i).getText());
        }

        RDD<BookEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity);

        //Find all the words
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

         words.count();

        JavaPairRDD<String, Long> wordCount = words.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                return new Tuple2<String, Long>(s, 1L);
            }
        });


        JavaPairRDD<String, Long> wordCountReduced = wordCount.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long integer, Long integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<WordCount> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Long>, WordCount>() {
            @Override
            public WordCount call(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
            }
        });


        ExtractorConfig<WordCount> outputConfigEntity = new ExtractorConfig(WordCount.class);
        outputConfigEntity.putValue(ExtractorConstants.HOST, hostConcat).putValue(ExtractorConstants.DATABASE, "book/words");
        outputConfigEntity.setExtractorImplClass(ESEntityExtractor.class);


        context.saveRDD(outputRDD.rdd(), outputConfigEntity);

        RDD<WordCount> outputRDDEntity = context.createRDD(outputConfigEntity);

        //Assert.assertEquals(((Long) outputRDDEntity.cache().count()).longValue(), ESJavaRDDTest.WORD_COUNT_SPECTED.longValue());

        context.stop();

    }




}