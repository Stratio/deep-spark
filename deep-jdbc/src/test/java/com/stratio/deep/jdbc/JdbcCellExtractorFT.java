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
import com.stratio.deep.core.extractor.ExtractorCellTest;
import com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Created by mariomgal on 11/12/14.
 */
@Test(groups = { "JdbcCellExtractorFT", "FunctionalTests" }, dependsOnGroups = { "JdbcJavaRDDFT" })
public class JdbcCellExtractorFT extends ExtractorCellTest {


    public JdbcCellExtractorFT() {
        super(JdbcNativeCellExtractor.class, JdbcJavaRDDFT.HOST, JdbcJavaRDDFT.PORT, true);
    }

    @Test
    @Override
    public void testDataSet() {
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<Cells> inputConfigEntity = getReadExtractorConfig();
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from input");
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number!=1");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<Cells> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number!=3");
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number=3");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<Cells> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number=2");
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select id from input");
            RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");
            Cells cells = inputRDDEntity.first();
            assertNotNull(cells.getCellByName("id"));
            assertNull(cells.getCellByName("message"));
            assertNull(cells.getCellByName("number"));

            ExtractorConfig<Cells> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select message, number from input");
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
    public ExtractorConfig getReadExtractorConfig() {
        ExtractorConfig<Cells> extractorConfig = super.getExtractorConfig(Cells.class);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_CELL)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, JdbcJavaRDDFT.INPUT_TABLE);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getWriteExtractorConfig(String tableOutput, Class entityClass) {
        ExtractorConfig<Cells> extractorConfig = super.getExtractorConfig(Cells.class);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_CELL)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, tableOutput);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getInputColumnConfig(String... inputColumns) {
        return this.getReadExtractorConfig();
    }

}
