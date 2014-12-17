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

import static com.stratio.deep.commons.utils.CellsUtils.getCellWithMapFromJson;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.extractor.ExtractorCellTest;

import scala.Tuple2;

/**
 * Created by rcrespo on 20/11/14.
 */
@Test(suiteName = "cassandraExtractorTests", groups = { "CassandraCellExtractorFT" }, dependsOnGroups = {
        "cassandraJavaRDDFT" })
public class CassandraCellExtractorFT extends ExtractorCellTest {

    private static final long serialVersionUID = -5201767473793541285L;

    public CassandraCellExtractorFT() {
        super(CassandraCellExtractor.class, "127.0.0.1", 9042, true);
    }

    @Override
    protected Cells transform(JSONObject jsonObject, String nameSpace, Class entityClass) {
        try {
            return getCellWithMapFromJson(jsonObject, "book.input");
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DeepTransformException(e.getMessage());
        }
    }

    @Override
    protected void initDataSetDivineComedy(DeepSparkContext context) {
        JavaRDD<String> stringJavaRDD;

        //Divine Comedy
        List<String> lineas = readFile("/simpleDivineComedy.json");

        stringJavaRDD = context.parallelize(lineas);

        JavaRDD javaRDD = transformRDD(stringJavaRDD, Cells.class);

        originBook = javaRDD.first();

        DeepSparkContext.saveRDD(javaRDD.rdd(), getWriteExtractorConfig(BOOK_INPUT,
                Cells.class));
    }

    @Test
    public void testDataSet() {

        DeepSparkContext context = getDeepSparkContext();

        try {

            ExtractorConfig<Cells> inputConfigEntity = getReadExtractorConfig(databaseExtractorName, BOOK_INPUT,
                    Cells.class);

            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(1, inputRDDEntity.count());

            List<Cells> books = inputRDDEntity.toJavaRDD().collect();

            Cells book = books.get(0);

            //      tests subDocuments

            assertEquals((book.getCellByName("metadata").getValue()), ((Cells) originBook).getCellByName("metadata")
                    .getValue());

            //      tests List<subDocuments>
            List<String> listCantos = (List<String>) ((Cells) originBook).getCellByName("cantos").getValue();

            for (int i = 0; i < listCantos.size(); i++) {
                String s = listCantos.get(i);
                assertEquals(s,
                        ((List<String>) book.getCellByName("cantos").getCellValue()).get(i));
                assertEquals(s,
                        ((List<String>) book.getCellByName("cantos").getCellValue()).get(i));
            }

            RDD<Cells> inputRDDEntity2 = context.createRDD(inputConfigEntity);

            JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<Cells, String>() {
                @Override
                public Iterable<String> call(Cells bookEntity) throws Exception {

                    List<String> words = new ArrayList<>();
                    for (String canto : ((List<String>) bookEntity.getCellByName("cantos").getCellValue())) {
                        words.addAll(Arrays.asList(canto.split(" ")));
                    }
                    return words;
                }
            });

            JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<>(s, 1);
                }
            });

            JavaPairRDD<String, Integer> wordCountReduced = wordCount
                    .reduceByKey(new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer integer, Integer integer2) throws Exception {
                            return integer + integer2;
                        }
                    });

            JavaRDD<Cells> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Integer>, Cells>() {
                @Override
                public Cells call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return new Cells(Cell.create("word", stringIntegerTuple2._1() != null && !stringIntegerTuple2._1()
                                    .isEmpty() ? stringIntegerTuple2._1()
                                    : "space",
                            true, true),
                            Cell.create("count", stringIntegerTuple2._2()));
                }
            });

            ExtractorConfig<Cells> outputConfigEntity = getWriteExtractorConfig(BOOK_OUTPUT, Cells.class);

            context.saveRDD(outputRDD.rdd(), outputConfigEntity);

            RDD<Cells> outputRDDEntity = context.createRDD(outputConfigEntity);

            Assert.assertEquals(WORD_COUNT_SPECTED.longValue() - 1l,
                    ((Long) outputRDDEntity.cache().count()).longValue()
            );
        } finally {
            context.stop();
        }

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
