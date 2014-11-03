package com.stratio.deep.aerospike.extractor;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.extractor.ExtractorTest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

@Test(suiteName = "aerospikeRddTests", groups = {"AerospikeCellExtractorTest"} , dependsOnGroups = "AerospikeJavaRDDTest")
public class AerospikeCellExtractorTest extends ExtractorTest {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeCellExtractorTest.class);

    public AerospikeCellExtractorTest() {
        super(AerospikeCellExtractor.class, "localhost:3000", null, true);
    }

    @Test
    public void testDataSet() {
        String hostConcat = AerospikeJavaRDDTest.HOST.concat(":").concat(AerospikeJavaRDDTest.PORT.toString());

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<Cells> inputConfigEntity = new ExtractorConfig(Cells.class);
            inputConfigEntity.putValue(ExtractorConstants.HOST, hostConcat).putValue(ExtractorConstants.DATABASE, "test")
                    .putValue(ExtractorConstants.COLLECTION, "books");
            inputConfigEntity.setExtractorImplClass(AerospikeCellExtractor.class);

            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(2, inputRDDEntity.count());

//            List<Cells> books = inputRDDEntity.toJavaRDD().collect();
//
//            Cells book = books.get(0);
//
//            DBObject proObject = new BasicDBObject();
//            proObject.put("_id", 0);
//
//            //      tests subDocuments
//            BSONObject result = MongoJavaRDDTest.mongo.getDB("book").getCollection("input").findOne(null, proObject);
//            assertEquals(((DBObject) result.get("metadata")).get("author"),
//                    ((Cells) book.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
//
//            //      tests List<subDocuments>
//            List<BSONObject> listCantos = (List<BSONObject>) result.get("cantos");
//
//            for (int i = 0; i < listCantos.size(); i++) {
//                BSONObject bsonObject = listCantos.get(i);
//                assertEquals(bsonObject.get("canto"),
//                        ((List<Cells>) book.getCellByName("cantos").getCellValue()).get(i).getCellByName("canto")
//                                .getCellValue());
//                assertEquals(bsonObject.get("text"),
//                        ((List<Cells>) book.getCellByName("cantos").getCellValue()).get(i).getCellByName("text")
//                                .getCellValue());
//            }
//
//            RDD<Cells> inputRDDEntity2 = context.createRDD(inputConfigEntity);
//
//            JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<Cells, String>() {
//                @Override
//                public Iterable<String> call(Cells bookEntity) throws Exception {
//
//                    List<String> words = new ArrayList<>();
//                    for (Cells canto : ((List<Cells>) bookEntity.getCellByName("cantos").getCellValue())) {
//                        words.addAll(Arrays.asList(((String) canto.getCellByName("text").getCellValue()).split(" ")));
//                    }
//                    return words;
//                }
//            });
//
//            JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
//                @Override
//                public Tuple2<String, Integer> call(String s) throws Exception {
//                    return new Tuple2<>(s, 1);
//                }
//            });
//
//            JavaPairRDD<String, Integer> wordCountReduced = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer integer, Integer integer2) throws Exception {
//                    return integer + integer2;
//                }
//            });
//
//            JavaRDD<Cells> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Integer>, Cells>() {
//                @Override
//                public Cells call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                    return new Cells(Cell.create("word", stringIntegerTuple2._1()),
//                            Cell.create("count", stringIntegerTuple2._2()));
//                }
//            });
//
//            ExtractorConfig<Cells> outputConfigEntity = new ExtractorConfig(Cells.class);
//            outputConfigEntity.putValue(ExtractorConstants.HOST, hostConcat).putValue(ExtractorConstants.DATABASE, "book")
//                    .putValue(ExtractorConstants.COLLECTION, "outputCell");
//            outputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);
//
//            context.saveRDD(outputRDD.rdd(), outputConfigEntity);
//
//            RDD<Cells> outputRDDEntity = context.createRDD(outputConfigEntity);
//
//            Assert.assertEquals(((Long) outputRDDEntity.cache().count()).longValue(),
//                    MongoJavaRDDTest.WORD_COUNT_SPECTED.longValue());
        }finally {
            context.stop();
        }



    }
}
