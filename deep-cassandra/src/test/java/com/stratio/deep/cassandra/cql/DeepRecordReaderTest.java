/**
 * 
 */
package com.stratio.deep.cassandra.cql;

import java.io.Serializable;
import java.util.Arrays;

import org.testng.annotations.Test;

import com.stratio.deep.cassandra.filter.value.EqualsInValue;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.rdd.DeepJavaRDD;

/**
 *
 */
@Test
public class DeepRecordReaderTest {

    @Test
    public void testDeepRecordReader() {

        String[] inputColumns = { "id", "split" };
        ExtractorConfig<Cells> extractorConfig = new ExtractorConfig<>(Cells.class);
        extractorConfig.putValue(ExtractorConstants.HOST, "127.0.0.1")
                .putValue(ExtractorConstants.DATABASE, "demo")
                .putValue(ExtractorConstants.PORT, 9160)
                .putValue(ExtractorConstants.COLLECTION, "splits")
                .putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
        extractorConfig.setExtractorImplClass(com.stratio.deep.cassandra.extractor.CassandraCellExtractor.class);

        Serializable[] inValues = { 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L };
        EqualsInValue equalsInValue = new EqualsInValue("id", 1L, "split", Arrays.asList(inValues));
        extractorConfig.putValue(ExtractorConstants.EQUALS_IN_FILTER, equalsInValue);

        DeepSparkContext context = new DeepSparkContext("local", "testAppName");
        DeepJavaRDD rdd = (DeepJavaRDD) context.createJavaRDD(extractorConfig);

        long elements = rdd.count();
        System.out.println("Elements: " + elements);
    }
}
