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

        Serializable[] inValues = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        EqualsInValue equalsInValue = new EqualsInValue(1, Arrays.asList(inValues));
        extractorConfig.putValue(ExtractorConstants.EQUALS_IN_FILTER, equalsInValue);

        DeepSparkContext context = new DeepSparkContext("local", "testAppName");
        DeepJavaRDD rdd = (DeepJavaRDD) context.createJavaRDD(extractorConfig);

        long elements = rdd.count();
        System.out.println("Elements: " + elements);
    }
}
