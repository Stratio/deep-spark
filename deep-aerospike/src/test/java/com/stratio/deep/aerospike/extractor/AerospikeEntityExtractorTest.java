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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.Record;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.extractor.ExtractorTest;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Created by mariomgal on 07/11/14.
 */
@Test(suiteName = "aerospikeRddTests", groups = {"AerospikeEntityExtractorTest"} , dependsOnGroups = "AerospikeJavaRDDTest")
public class AerospikeEntityExtractorTest extends ExtractorTest {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeCellExtractorTest.class);

    public AerospikeEntityExtractorTest() {
        super(AerospikeCellExtractor.class, "127.0.0.1", 3000, false);
    }

    @Test
    public void testDataSet() {

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {

            Statement stmnt = new Statement();
            stmnt.setNamespace("book");
            stmnt.setSetName("input");

            stmnt.setFilters(Filter.equal("id", "TestDataSet"));

            RecordSet recordSet = AerospikeJavaRDDTest.aerospike.query(null, stmnt);
            Record record = null;
            while(recordSet.next()) {
                record = recordSet.getRecord();
            }
            AerospikeRecord aerospikeRecord = new AerospikeRecord(record);

            Map<String, Object> bins = aerospikeRecord.bins;

            ExtractorConfig<BookEntity> inputConfigEntity = new ExtractorConfig(BookEntity.class);
            inputConfigEntity.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "book")
                    .putValue(ExtractorConstants.SET, "input");
            inputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<BookEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(1, inputRDDEntity.count());

            List<BookEntity> books = inputRDDEntity.toJavaRDD().collect();

            BookEntity book = books.get(0);



            assertEquals(((Map<String, String>)bins.get("metadata")).get("author"), book.getMetadataEntity().getAuthor());

//            //      tests List<subDocuments>
//            List<BSONObject> listCantos = (List<BSONObject>) result.get("cantos");
//
//            for (int i = 0; i < listCantos.size(); i++) {
//                BSONObject bsonObject = listCantos.get(i);
//                assertEquals(bsonObject.get("canto"), book.getCantoEntities().get(i).getNumber());
//                assertEquals(bsonObject.get("text"), book.getCantoEntities().get(i).getText());
//            }
//
//            RDD<BookEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity);
//
//            JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<BookEntity, String>() {
//                @Override
//                public Iterable<String> call(BookEntity bookEntity) throws Exception {
//
//                    List<String> words = new ArrayList<>();
//                    for (CantoEntity canto : bookEntity.getCantoEntities()) {
//                        words.addAll(Arrays.asList(canto.getText().split(" ")));
//                    }
//                    return words;
//                }
//            });
//
//            JavaPairRDD<String, Long> wordCount = words.mapToPair(new PairFunction<String, String, Long>() {
//                @Override
//                public Tuple2<String, Long> call(String s) throws Exception {
//                    return new Tuple2<String, Long>(s, 1l);
//                }
//            });
//
//            JavaPairRDD<String, Long> wordCountReduced = wordCount.reduceByKey(new Function2<Long, Long, Long>() {
//                @Override
//                public Long call(Long integer, Long integer2) throws Exception {
//                    return integer + integer2;
//                }
//            });
//
//            JavaRDD<WordCount> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Long>, WordCount>() {
//                @Override
//                public WordCount call(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
//                    return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
//                }
//            });
//
//            ExtractorConfig<WordCount> outputConfigEntity = new ExtractorConfig(WordCount.class);
//            outputConfigEntity.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
//                    .putValue(ExtractorConstants.NAMESPACE, "book")
//                    .putValue(ExtractorConstants.SET, "output");
//            outputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);
//
//            context.saveRDD(outputRDD.rdd(), outputConfigEntity);
//
//            RDD<WordCount> outputRDDEntity = context.createRDD(outputConfigEntity);
//
//            Assert.assertEquals(((Long) outputRDDEntity.cache().count()).longValue(),
//                    AerospikeJavaRDDTest.WORD_COUNT_SPECTED.longValue());

        }finally {
            context.stop();
        }


    }
}
