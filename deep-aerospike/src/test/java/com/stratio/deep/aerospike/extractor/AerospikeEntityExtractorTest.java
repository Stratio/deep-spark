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
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.extractor.ExtractorEntityTest;
import com.stratio.deep.core.extractor.ExtractorTest;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Created by mariomgal on 07/11/14.
 */
@Test(suiteName = "aerospikeRddTests", groups = {"AerospikeEntityExtractorTest"} , dependsOnGroups = "AerospikeJavaRDDTest")
public class AerospikeEntityExtractorTest extends ExtractorEntityTest {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeEntityExtractorTest.class);

    public AerospikeEntityExtractorTest() {
        super(AerospikeEntityExtractor.class, "127.0.0.1", 3000, false);
    }

    @Override
    @Test
    public void testInputColumns() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {

            ExtractorConfig<MessageTestEntity> inputConfigEntity = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input").putValue(ExtractorConstants.INPUT_COLUMNS, new String[] { "_id" });
            inputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            MessageTestEntity messageEntity = inputRDDEntity.first();

            assertNotNull(messageEntity.getId(), "Message id should not be null.");
            assertNull(messageEntity.getMessage(), "Message text should be null.");

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity2.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input").putValue(ExtractorConstants.INPUT_COLUMNS, new String[] { "message" });
            inputConfigEntity2.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            MessageTestEntity messageEntity2 = inputRDDEntity2.first();

            assertNull(messageEntity2.getId(), "Message id should be null.");
            assertNotNull(messageEntity2.getMessage(), "Message text should not be null.");

        }finally {
            context.stop();
        }
    }

    @Test
    public void testDataSet() {

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {

            Statement stmnt = new Statement();
            stmnt.setNamespace("test");
            stmnt.setSetName("input");

            stmnt.setFilters(Filter.equal("_id", "3"));

            RecordSet recordSet = AerospikeJavaRDDTest.aerospike.query(null, stmnt);
            Record record = null;
            while(recordSet.next()) {
                record = recordSet.getRecord();
            }
            AerospikeRecord aerospikeRecord = new AerospikeRecord(record);

            Map<String, Object> bins = aerospikeRecord.bins;

            ExtractorConfig<MessageTestEntity> inputConfigEntity = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input");
            inputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(1, inputRDDEntity.count(), "There should be 1 read entity.");

            List<MessageTestEntity> messages = inputRDDEntity.toJavaRDD().collect();

            MessageTestEntity message = messages.get(0);

            assertEquals((String)bins.get("_id"), message.getId(), "Read message id should be the same as expected.");
            assertEquals((String)bins.get("message"), message.getMessage(), "Read message text should be the same as expected.");

        }finally {
            context.stop();
        }
    }

    @Override
    @Test
    protected void testFilter() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {

            com.stratio.deep.commons.filter.Filter[] filters = null;
            com.stratio.deep.commons.filter.Filter equalFilter = new com.stratio.deep.commons.filter.Filter("number", FilterType.EQ, 3L);
            com.stratio.deep.commons.filter.Filter ltFilter = new com.stratio.deep.commons.filter.Filter("number", FilterType.LT, 4L);
            com.stratio.deep.commons.filter.Filter gtFilter = new com.stratio.deep.commons.filter.Filter("number", FilterType.GT, 5L);
            com.stratio.deep.commons.filter.Filter lteFilter = new com.stratio.deep.commons.filter.Filter("number",FilterType.LTE, 3L);
            com.stratio.deep.commons.filter.Filter gteFilter = new com.stratio.deep.commons.filter.Filter("number",FilterType.GTE, 4L);
            com.stratio.deep.commons.filter.Filter equalFilter2 = new com.stratio.deep.commons.filter.Filter("number", FilterType.EQ, 4L);

            try {
                filters = new com.stratio.deep.commons.filter.Filter[] { equalFilter, ltFilter };
                ExtractorConfig inputConfigEntity = getFilterConfig(filters);
            } catch(UnsupportedOperationException e) {
                LOG.info("Expected exception thrown for more than one filter in aerospike");
            }

            try {
                filters = new com.stratio.deep.commons.filter.Filter[] { new com.stratio.deep.commons.filter.Filter("number", FilterType.NEQ, "invalid")};
                ExtractorConfig inputConfigEntity = getFilterConfig(filters);
            } catch(UnsupportedOperationException e) {
                LOG.info("Expected exception thrown for a filter not supported by aerospike");
            }

            try {
                com.stratio.deep.commons.filter.Filter invalidFilter = new com.stratio.deep.commons.filter.Filter("number", FilterType.LT, "invalid");
                filters = new com.stratio.deep.commons.filter.Filter[] { invalidFilter };
                ExtractorConfig inputConfigEntity = getFilterConfig(filters);
            } catch(UnsupportedOperationException e) {
                LOG.info("Expected exception thrown for using a range filter without Long mandatory value type");
            }

            ExtractorConfig<MessageTestEntity> inputConfigEntity = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input")
                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {equalFilter});
            inputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);
            assertEquals(1, inputRDDEntity.count(), "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity2.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input")
                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {ltFilter});
            inputConfigEntity2.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity2);
            assertEquals(1, inputRDDEntity2.count(), "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity3 = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity3.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input")
                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {gtFilter});
            inputConfigEntity3.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity3 = context.createRDD(inputConfigEntity3);
            assertEquals(0, inputRDDEntity3.count(), "Expected read entity count is 0");

            ExtractorConfig<MessageTestEntity> inputConfigEntity4 = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity4.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input")
                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {lteFilter});
            inputConfigEntity4.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity4 = context.createRDD(inputConfigEntity4);
            assertEquals(1, inputRDDEntity4.count(), "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity5 = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity5.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input")
                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {gteFilter});
            inputConfigEntity5.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity5 = context.createRDD(inputConfigEntity5);
            assertEquals(0, inputRDDEntity5.count(), "Expected read entity count is 0");

            ExtractorConfig<MessageTestEntity> inputConfigEntity6 = new ExtractorConfig(MessageTestEntity.class);
            inputConfigEntity6.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "test")
                    .putValue(ExtractorConstants.SET, "input")
                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {gteFilter});
            inputConfigEntity6.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<MessageTestEntity> inputRDDEntity6 = context.createRDD(inputConfigEntity5);
            assertEquals(0, inputRDDEntity6.count(), "Expected read entity count is 0");

            //TODO: Equality filter not working properly
//            ExtractorConfig<MessageTestEntity> inputConfigEntity7 = new ExtractorConfig(MessageTestEntity.class);
//            inputConfigEntity7.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
//                    .putValue(ExtractorConstants.NAMESPACE, "test")
//                    .putValue(ExtractorConstants.SET, "input")
//                    .putValue(ExtractorConstants.FILTER_QUERY, new com.stratio.deep.commons.filter.Filter[] {equalFilter2});
//            inputConfigEntity7.setExtractorImplClass(AerospikeEntityExtractor.class);
//
//            RDD<MessageTestEntity> inputRDDEntity7 = context.createRDD(inputConfigEntity7);
//            assertEquals(0, inputRDDEntity7.count());

        }finally {
            context.stop();
        }
    }

    @Test
    public void testInvalidDataType() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {
            ExtractorConfig<BookEntity> inputConfigEntity = new ExtractorConfig(BookEntity.class);
            inputConfigEntity.putValue(ExtractorConstants.HOST, AerospikeJavaRDDTest.HOST).putValue(ExtractorConstants.PORT, AerospikeJavaRDDTest.PORT)
                    .putValue(ExtractorConstants.NAMESPACE, "book")
                    .putValue(ExtractorConstants.SET, "input");
            inputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);

            RDD<BookEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            BookEntity bookEntity = inputRDDEntity.first();

            fail();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            context.stop();
        }
    }
}
