package com.stratio.deep.commons.config; 



import static junit.framework.TestCase.assertEquals;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Test;

/** 
* ExtractorConfig Tester. 
* 
* @author <Authors name> 
* @since <pre>ene 27, 2015</pre> 
* @version 1.0 
*/ 
public class ExtractorConfigTest { 






/** 
* 
* Method: getInteger(String key) 
* 
*/ 
@Test
public void testGetInteger() throws Exception {

    String value = "1234";

    ExtractorConfig extractorConfig = new ExtractorConfig();
    Map<String, Serializable> values = new HashedMap();
    values.put(value,value);
    extractorConfig.setValues(values);

    Integer result = extractorConfig.getInteger(value);

    assertEquals("The result must be "+value, new Integer(value),result);

}

    @Test
    public void testGetIntegerWorkArroundPorts() throws Exception {

        String value = "1234,1234,1234";


        ExtractorConfig extractorConfig = new ExtractorConfig();
        Map<String, Serializable> values = new HashedMap();
        values.put(value,value);
        extractorConfig.setValues(values);

        Integer result = extractorConfig.getInteger(value);

        String expectedValue = value.split(",")[0];
        assertEquals("The result must be "+ expectedValue, new Integer(expectedValue),result);

    }



} 
