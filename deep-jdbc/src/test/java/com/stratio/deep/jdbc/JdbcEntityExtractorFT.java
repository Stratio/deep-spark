package com.stratio.deep.jdbc;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
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
 * Created by mariomgal on 11/12/14.
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from input");
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select id from input");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");
            MessageTestEntity message = inputRDDEntity.first();
            assertNotNull(message.getId());
            assertNull(message.getMessage());
            assertNull(message.getNumber());

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select message, number from input");
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number=3");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number=2");
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
            inputConfigEntity.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number!=1");
            RDD<MessageTestEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(inputRDDEntity.count(), 1, "Expected read entity count is 1");

            ExtractorConfig<MessageTestEntity> inputConfigEntity2 = getReadExtractorConfig();
            inputConfigEntity2.putValue(ExtractorConstants.JDBC_QUERY, "select * from input where number!=3");
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
        return this.getReadExtractorConfig();
    }

    @Override
    public ExtractorConfig getReadExtractorConfig() {
        ExtractorConfig<MessageTestEntity> extractorConfig = super.getExtractorConfig(MessageTestEntity.class);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_ENTITY)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, JdbcJavaRDDFT.INPUT_TABLE);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getWriteExtractorConfig(String tableOutput, Class entityClass) {
        ExtractorConfig<MessageTestEntity> extractorConfig = super.getExtractorConfig(MessageTestEntity.class);
        extractorConfig.putValue(ExtractorConstants.HOST, JdbcJavaRDDFT.HOST)
                .putValue(ExtractorConstants.USERNAME, JdbcJavaRDDFT.USER)
                .putValue(ExtractorConstants.PASSWORD, JdbcJavaRDDFT.PASSWORD)
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_ENTITY)
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
