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

package com.stratio.deep.core.extractor;

import static com.stratio.deep.commons.utils.CellsUtils.getCellFromJson;
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

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.core.context.DeepSparkContext;

import scala.Tuple2;

/**
 * Created by rcrespo on 17/11/14.
 */
public abstract class ExtractorCellTest<S extends BaseConfig<Cells>> extends ExtractorTest<Cells, S> {
    private static final long serialVersionUID = -7147600574221227223L;

    /**
     * @param extractor
     * @param host
     * @param port
     * @param isCells
     */
    public ExtractorCellTest(
            Class<IExtractor<Cells, S>> extractor, String host, Integer port,
            boolean isCells) {
        super(extractor, host, port, isCells);
    }

    @Override
    protected <T> T transform(JSONObject jsonObject, String nameSpace, Class<T> entityClass) {
        try {
            return (T) getCellFromJson(jsonObject, "book.input");
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DeepTransformException(e.getMessage());
        }
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

            assertEquals((book.getCellByName("metadata").getValue()), originBook.getCellByName("metadata").getValue());

            //      tests List<subDocuments>
            List<Cells> listCantos = (List<Cells>) originBook.getCellByName("cantos").getValue();

            for (int i = 0; i < listCantos.size(); i++) {
                Cells cells = listCantos.get(i);
                assertEquals(cells.getCellByName("canto").getValue(),
                        ((List<Cells>) book.getCellByName("cantos").getCellValue()).get(i).getCellByName("canto")
                                .getCellValue());
                assertEquals(cells.getCellByName("text").getValue(),
                        ((List<Cells>) book.getCellByName("cantos").getCellValue()).get(i).getCellByName("text")
                                .getCellValue());
            }

            RDD<Cells> inputRDDEntity2 = context.createRDD(inputConfigEntity);

            JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<Cells, String>() {
                @Override
                public Iterable<String> call(Cells bookEntity) throws Exception {

                    List<String> words = new ArrayList<>();
                    for (Cells canto : ((List<Cells>) bookEntity.getCellByName("cantos").getCellValue())) {
                        words.addAll(Arrays.asList(((String) canto.getCellByName("text").getCellValue()).split(" ")));
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
                    return new Cells(Cell.create("word", stringIntegerTuple2._1()),
                            Cell.create("count", stringIntegerTuple2._2()));
                }
            });

            ExtractorConfig<Cells> outputConfigEntity = getWriteExtractorConfig(BOOK_OUTPUT, Cells.class);

            context.saveRDD(outputRDD.rdd(), outputConfigEntity);

            RDD<Cells> outputRDDEntity = context.createRDD(outputConfigEntity);

            Assert.assertEquals(((Long) outputRDDEntity.cache().count()).longValue(),
                    WORD_COUNT_SPECTED.longValue());
        } finally {
            context.stop();
        }

    }
}
