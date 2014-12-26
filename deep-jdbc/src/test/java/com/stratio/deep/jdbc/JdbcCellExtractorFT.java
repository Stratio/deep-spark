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
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.extractor.ExtractorCellTest;
import com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor;
import org.apache.spark.rdd.RDD;
import org.testng.Assert;
import org.testng.annotations.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests for JdbcCellExtractor
 */
@Test(groups = { "JdbcCellExtractorFT", "FunctionalTests" }, dependsOnGroups = { "JdbcJavaRDDFT" })
public class JdbcCellExtractorFT extends ExtractorCellTest {


    public JdbcCellExtractorFT() {
        super(JdbcNativeCellExtractor.class, JdbcJavaRDDFT.HOST, JdbcJavaRDDFT.PORT, true);
    }

    @Test
    @Override
    public void testRead() {

        DeepSparkContext context = getDeepSparkContext();

        try {

            ExtractorConfig inputConfigEntity = getReadExtractorConfig(databaseExtractorName, tableRead,
                    Cells.class);
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "SELECT * FROM \"" + tableRead + "\"");

            RDD inputRDDEntity = context.createRDD(inputConfigEntity);

            Assert.assertEquals(READ_COUNT_EXPECTED, inputRDDEntity.count());

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(((Cells) inputRDDEntity.first()).getCellByName("message").getCellValue(),
                        READ_FIELD_EXPECTED);

                Assert.assertEquals(((Cells) inputRDDEntity.first()).getCellByName("id").getCellValue(),
                        ID_MESSAGE_EXPECTED);
            } else {
                Assert.assertEquals(((MessageTestEntity) inputRDDEntity.first()).getMessage(), READ_FIELD_EXPECTED);

                Assert.assertEquals(((MessageTestEntity) inputRDDEntity.first()).getId(), ID_MESSAGE_EXPECTED);
            }

        } finally {
            context.stop();
        }

    }

    @Test
    @Override
    public void testWrite() {

        DeepSparkContext context = getDeepSparkContext();

        try {

            ExtractorConfig inputConfigEntity = getReadExtractorConfig(databaseExtractorName, tableRead,
                    Cells.class);
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "SELECT * FROM \"" + tableRead + "\"");

            RDD inputRDDEntity = context.createRDD(inputConfigEntity);

            ExtractorConfig outputConfigEntity;
            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                outputConfigEntity = getWriteExtractorConfig("outputCells", Cells.class);
            } else {
                outputConfigEntity = getWriteExtractorConfig("outputEntity", MessageTestEntity.class);
            }

            // Save RDD in DataSource
            context.saveRDD(inputRDDEntity, outputConfigEntity);

            RDD outputRDDEntity = context.createRDD(outputConfigEntity);

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(((Cells) outputRDDEntity.first()).getCellByName("message").getCellValue(),
                        READ_FIELD_EXPECTED);
            } else {

                Assert.assertEquals(((MessageTestEntity) outputRDDEntity.first()).getMessage(), READ_FIELD_EXPECTED);
            }
        } finally {
            context.stop();
        }

    }

    @Test
    @Override
    public void testDataSet() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<Cells> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\"");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

        } finally {
            context.stop();
        }

    }

    @Test
    @Override
    public void testFilterNEQ() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<Cells> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"!=1");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<Cells> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"!=3");
            RDD<Cells> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity2.count(), 0, "Expected read entity count is 1");

        } finally {
            context.stop();
        }
    }

    @Test
    @Override
    public void testFilterEQ() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<Cells> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"=3");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<Cells> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from \"input\" where \"number\"=2");
            RDD<Cells> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity2.count(), 0, "Expected read entity count is 1");

        } finally {
            context.stop();
        }
    }

    @Test
    @Override
    public void testInputColumns() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<Cells> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select \"id\" from \"input\"");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");
            Cells cells = inputRDDEntity.first();
            assertNotNull(cells.getCellByName("id"));
            assertNull(cells.getCellByName("message"));
            assertNull(cells.getCellByName("number"));

            ExtractorConfig<Cells> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select \"message\", \"number\" from \"input\"");
            RDD<Cells> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            assertEquals(inputRDDEntity2.count(), 1, "Expected read entity count is 1");
            Cells cells2 = inputRDDEntity2.first();
            assertNull(cells2.getCellByName("id"));
            assertNotNull(cells2.getCellByName("message"));
            assertNotNull(cells2.getCellByName("number"));

        } finally {
            context.stop();
        }
    }


    @Override
    public ExtractorConfig getReadExtractorConfig(String database, String collection, Class entityClass) {
        return this.getReadExtractorConfig();
    }

    @Override
    protected void initDataSetDivineComedy(DeepSparkContext context) {

    }

    @Override
    public ExtractorConfig  getReadExtractorConfig() {
        ExtractorConfig<Cells> extractorConfig = super.getExtractorConfig(Cells.class);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.JDBC_CONNECTION_URL, "jdbc:hsqldb:file:" + JdbcJavaRDDFT.NAMESPACE_CELL)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_CELL)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, JdbcJavaRDDFT.INPUT_TABLE)
                .putValue(ExtractorConstants.JDBC_QUOTE_SQL, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getWriteExtractorConfig(String tableOutput, Class entityClass) {
        ExtractorConfig<Cells> extractorConfig = super.getExtractorConfig(Cells.class);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.JDBC_CONNECTION_URL, "jdbc:hsqldb:file:" + JdbcJavaRDDFT.NAMESPACE_CELL)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_CELL)
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
