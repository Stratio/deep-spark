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

package com.stratio.deep.cassandra;

import static com.stratio.deep.commons.utils.CellsUtils.getObjectWithMapFromJson;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.json.simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.stratio.deep.cassandra.extractor.CassandraEntityExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.SimpleBookEntity;
import com.stratio.deep.core.entity.WordCount;
import com.stratio.deep.core.extractor.ExtractorEntityTest;

import scala.Tuple2;

/**
 * Created by rcrespo on 24/11/14.
 */
@Test(suiteName = "cassandraExtractorTests", groups = { "CassandraEntityExtractorFT" }, dependsOnGroups = {
        "CassandraCellExtractorFT" })
public class CassandraEntityExtractorFT extends ExtractorEntityTest {

    private static final long serialVersionUID = -172112587882501217L;

    public CassandraEntityExtractorFT() {
        super(CassandraEntityExtractor.class, "localhost", 9242, false, SimpleBookEntity.class);
    }

    public Object transform(JSONObject jsonObject, String nameSpace, Class entityClass) {
        try {
            return getObjectWithMapFromJson(entityClass, jsonObject);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void initDataSetDivineComedy(DeepSparkContext context) {
        JavaRDD<String> stringJavaRDD;

        //Divine Comedy
        List<String> lineas = readFile("/simpleDivineComedy.json");

        stringJavaRDD = context.parallelize(lineas);

        JavaRDD javaRDD = transformRDD(stringJavaRDD, SimpleBookEntity.class);

        originBook = javaRDD.first();

        DeepSparkContext.saveRDD(javaRDD.rdd(), getWriteExtractorConfig(BOOK_INPUT,
                SimpleBookEntity.class));
    }

    @Test
    public void testDataSet() {

        DeepSparkContext context = getDeepSparkContext();
        try {

            ExtractorConfig<SimpleBookEntity> inputConfigEntity = getReadExtractorConfig(databaseExtractorName,
                    BOOK_INPUT,
                    SimpleBookEntity.class);

            RDD<SimpleBookEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(1, inputRDDEntity.count());

            List<SimpleBookEntity> books = inputRDDEntity.toJavaRDD().collect();

            SimpleBookEntity book = books.get(0);

            //                  tests subDocuments

            Map<String, String> originMap = ((SimpleBookEntity) originBook).getMetadata();

            Map<String, String> bookMap = book.getMetadata();

            Set<String> keys = originMap.keySet();

            assertEquals(keys, bookMap.keySet());
            for (String key : keys) {
                assertEquals(bookMap.get(key), originMap.get(key));

            }

            //      tests List<subDocuments>
            List<String> listCantos = ((SimpleBookEntity) originBook).getCantos();

            for (int i = 0; i < listCantos.size(); i++) {
                assertEquals(listCantos.get(i), book.getCantos().get(i));
            }

            RDD<SimpleBookEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity);

            JavaRDD<String> words = inputRDDEntity2.toJavaRDD()
                    .flatMap(new FlatMapFunction<SimpleBookEntity, String>() {
                        @Override
                        public Iterable<String> call(SimpleBookEntity bookEntity) throws Exception {

                            List<String> words = new ArrayList<>();
                            for (String canto : bookEntity.getCantos()) {
                                words.addAll(Arrays.asList(canto.split(" ")));
                            }
                            return words;
                        }
                    });

            JavaPairRDD<String, Long> wordCount = words.mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {
                    return new Tuple2<String, Long>(s, 1l);
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

            ExtractorConfig<WordCount> outputConfigEntity = getWriteExtractorConfig(BOOK_OUTPUT, WordCount.class);

            context.saveRDD(outputRDD.rdd(), outputConfigEntity);

            RDD<WordCount> outputRDDEntity = context.createRDD(outputConfigEntity);

            Assert.assertEquals(WORD_COUNT_SPECTED.longValue() - 1l, ((Long) outputRDDEntity.cache().count()).longValue
                    ());

        } finally {
            context.stop();
        }

    }

    //TODO
    @Override
    public void testFilterEQ() {

    }

    //TODO
    @Override
    public void testFilterNEQ() {

    }

    //TODO
    @Override
    public void testInputColumns() {

    }
}
