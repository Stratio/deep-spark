package com.stratio.deep.jdbc;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.extractor.ExtractorCellTest;
import com.stratio.deep.core.extractor.ExtractorTest;
import com.stratio.deep.jdbc.extractor.JdbcNativeCellExtractor;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

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

    }

    @Test
    @Override
    public void testFilterEQ() {

    }

    @Test
    @Override
    public void testInputColumns() {

    }

    @Test
    @Override
    public void testRead() {

    }

    @Test
    @Override
    public void testWrite() {

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
                .putValue(ExtractorConstants.CATALOG, JdbcJavaRDDFT.NAMESPACE_CELL)
                .putValue(ExtractorConstants.JDBC_DRIVER_CLASS, JdbcJavaRDDFT.DRIVER_CLASS)
                .putValue(ExtractorConstants.PORT, JdbcJavaRDDFT.PORT)
                .putValue(ExtractorConstants.TABLE, JdbcJavaRDDFT.SET_NAME);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    @Override
    public ExtractorConfig getWriteExtractorConfig(String tableOutput, Class entityClass) {
        return super.getWriteExtractorConfig(tableOutput, entityClass);
    }

    @Override
    public ExtractorConfig getInputColumnConfig(String... inputColumns) {
        return this.getReadExtractorConfig();
    }

}
