package com.stratio.deep.config;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.extractor.utils.ExtractorConfig;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by dgomez on 20/08/14.
 */
@Test
public class DeepJobConfigTest {

    @Test
    public void testCreate() {

        DeepJobConfig<Cells> config = new DeepJobConfig();

        assertEquals(config.getValues().size(),0);


        Map<ExtractorConfig, String> values = new HashMap<ExtractorConfig,String>();
        values.put(ExtractorConfig.KEYSPACE, "twitter");
        values.put(ExtractorConfig.TABLE, "tweets");
        values.put(ExtractorConfig.CQLPORT, "9042");
        values.put(ExtractorConfig.RPCPORT, "9160");
        values.put(ExtractorConfig.HOST, "127.0.0.1");

        config.setValues(values);

        assertTrue(config.getValues().containsKey(ExtractorConfig.KEYSPACE));
        assertTrue(config.getValues().containsKey(ExtractorConfig.TABLE));
        assertTrue(config.getValues().containsKey(ExtractorConfig.CQLPORT));
        assertTrue(config.getValues().containsKey(ExtractorConfig.RPCPORT));
        assertTrue(config.getValues().containsKey(ExtractorConfig.HOST));

        assertFalse(config.getValues().containsKey(ExtractorConfig.INPUT_COLUMNS));
        assertFalse(config.getValues().containsKey(ExtractorConfig.PAGE_SIZE));
        assertFalse(config.getValues().containsKey(ExtractorConfig.PASSWORD));
        assertFalse(config.getValues().containsKey(ExtractorConfig.PORT));
        assertFalse(config.getValues().containsKey(ExtractorConfig.SESSION));
        assertFalse(config.getValues().containsKey(ExtractorConfig.USERNAME));

        config.setEntityClass(null);

        assertEquals(config.getEntityClass(), null);

        //config.setConfig();
    }

}
