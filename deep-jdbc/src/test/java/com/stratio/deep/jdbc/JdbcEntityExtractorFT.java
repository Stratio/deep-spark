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

package com.stratio.deep.jdbc;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.extractor.ExtractorEntityTest;
import com.stratio.deep.jdbc.extractor.JdbcNativeEntityExtractor;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests for JdbcEntityExtractor
 */
@Test(groups = { "JdbcEntityExtractorFT", "FunctionalTests" }, dependsOnGroups = { "JdbcJavaRDDFT" })
public class JdbcEntityExtractorFT extends ExtractorEntityTest {

    public JdbcEntityExtractorFT() {
        super(JdbcNativeEntityExtractor.class, JdbcJavaRDDFT.HOST, JdbcJavaRDDFT.PORT, false);
    }

    @Test
    @Override
    public void testDataSet() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<MessageTestEntity> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\"");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

        } finally {
            context.stop();
        }

    }

    @Test
    @Override
    public void testInputColumns() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<MessageTestEntity> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select \"id\" from \"input\"");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");
            MessageTestEntity message = inputRDDEntity.first();
            assertNotNull(message.getId());
            assertNull(message.getMessage());
            assertNull(message.getNumber());

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select \"message\", \"number\" from \"input\"");
            RDD<MessageTestEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            assertEquals(inputRDDEntity2.count(), 1, "Expected read entity count is 1");
            MessageTestEntity message2 = inputRDDEntity2.first();
            assertNull(message2.getId());
            assertNotNull(message2.getMessage());
            assertNotNull(message2.getNumber());

        } finally {
            context.stop();
        }
    }

    @Test
    @Override
    public void testFilterEQ() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<MessageTestEntity> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"=3");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"=2");
            RDD<MessageTestEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity2.count(), 0, "Expected read entity count is 1");

        } finally {
            context.stop();
        }
    }

    @Test
    @Override
    public void testFilterNEQ() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<MessageTestEntity> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"!=1");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"!=3");
            RDD<MessageTestEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity2.count(), 0, "Expected read entity count is 1");

        } finally {
            context.stop();
        }
    }

    @Override
    protected void initDataSetDivineComedy(DeepSparkContext context) {

    }

    @Override
    public ExtractorConfig getReadExtractorConfig(String database, String collection, Class entityClass) {
        ExtractorConfig extractorConfig = super.getExtractorConfig(entityClass);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.JDBC_CONNECTION_URL, "jdbc:hsqldb:mem:" + JdbcJavaRDDFT.NAMESPACE_ENTITY)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_ENTITY)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, collection)
                .putValue(ExtractorConstants.JDBC_QUOTE_SQL, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getReadExtractorConfig() {
        return this.getReadExtractorConfig(JdbcJavaRDDFT.NAMESPACE_ENTITY, JdbcJavaRDDFT.INPUT_TABLE,
                MessageTestEntity.class);
    }

    @Override
    public  ExtractorConfig getWriteExtractorConfig(String tableOutput, Class entityClass) {
        ExtractorConfig extractorConfig = super.getExtractorConfig(entityClass);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.JDBC_CONNECTION_URL, "jdbc:hsqldb:mem:" + JdbcJavaRDDFT.NAMESPACE_ENTITY)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_ENTITY)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, tableOutput)
                .putValue(ExtractorConstants.JDBC_QUOTE_SQL, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getInputColumnConfig(String... inputColumns) {
        return this.getReadExtractorConfig();
    }
}
