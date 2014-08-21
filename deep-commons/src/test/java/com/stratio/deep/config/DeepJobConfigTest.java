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

        assertEquals(config.getValues(),null);


        Map<String, String> values = new HashMap<String,String>();
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

//        assertTrue(!config.getValues().containsKey(ExtractorConfig.INPUT_COLUMNS));
//        assertTrue(!config.getValues().containsKey(ExtractorConfig.PAGE_SIZE));
//        assertTrue(!config.getValues().containsKey(ExtractorConfig.PASSWORD));
//        assertTrue(!config.getValues().containsKey(ExtractorConfig.PORT));
//        assertTrue(!config.getValues().containsKey(ExtractorConfig.SESSION));
//        assertTrue(!config.getValues().containsKey(ExtractorConfig.USERNAME));

        config.setEntityClass(null);

        assertEquals(config.getEntityClass(), null);

        //config.setConfig();
    }

}
