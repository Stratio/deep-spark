package com.stratio.deep.rdd; 

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.extractor.client.ExtractorClient;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import scala.collection.Iterator;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Method;

/** 
* DeepRDD Tester. 
* 
* @author <Authors name> 
* @since <pre>ago 22, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {ExtractorClient.class,DeepRDD.class})
public class DeepRDDTest {

    public static final Object FIRST_RESULT = "FirstResult";
    private static final Object SECOND_RESULT = "SecondResult";

    @Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: compute(Partition split, TaskContext context) 
* 
*/ 
@Test
public void testCompute() throws Exception {
    DeepRDD deepRDD = createDeepRDD();

    DeepTokenRange deepTokenRange = mock(DeepTokenRange.class);
    ExtractorConfig extractorConfig = mock(ExtractorConfig.class);

    createExtractorClient(deepTokenRange, extractorConfig);
    Broadcast config = createConfig(extractorConfig);
    deepRDD.config = config;

    IDeepPartition partition = createPartition(deepTokenRange);
    TaskContext taskcontext = createTaskContext();



    Iterator iterator = deepRDD.compute(partition, taskcontext);

    assertNotNull("iterator is not null",iterator);
    assertTrue("Iterator has one object", iterator.hasNext());
    assertEquals("The first object in the iterator is correct", FIRST_RESULT, iterator.next());
    assertTrue("Iterator has two object", iterator.hasNext());
    assertEquals("The second object in the iterator is correct", SECOND_RESULT, iterator.next());
    assertFalse("Iterator has not next",iterator.hasNext());


}

    private TaskContext createTaskContext() {
        TaskContext taskcontext = mock(TaskContext.class);
        doNothing().when(taskcontext).addOnCompleteCallback(any(OnComputedRDDCallback.class));
        return taskcontext;
    }

    private IDeepPartition createPartition(DeepTokenRange deepTokenRange) {
        IDeepPartition partition = mock(IDeepPartition.class);

        when(partition.splitWrapper()).thenReturn(deepTokenRange);
        return partition;
    }

    private Broadcast createConfig(ExtractorConfig extractorConfig) {
        Broadcast config = mock(Broadcast.class);


        when(config.getValue()).thenReturn(extractorConfig);
        return config;
    }

    private void createExtractorClient(DeepTokenRange deepTokenRange, ExtractorConfig extractorConfig) throws Exception {
        ExtractorClient extractorClient = mock(ExtractorClient.class);
        whenNew(ExtractorClient.class).withAnyArguments().thenReturn(extractorClient);
        doNothing().when(extractorClient).initialize();
        when(extractorClient.hasNext()).thenReturn(true,true,false);
        when(extractorClient.next()).thenReturn(FIRST_RESULT, SECOND_RESULT);
        doNothing().when(extractorClient).initIterator(deepTokenRange, extractorConfig);
    }

    /**
* 
* Method: getPartitions() 
* 
*/ 
@Test
public void testGetPartitions() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getConfig() 
* 
*/ 
@Test
public void testGetConfig() throws Exception { 
//TODO: Test goes here... 
} 


/** 
* 
* Method: initExtractorClient() 
* 
*/ 
@Test
public void testInitExtractorClient() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = DeepRDD.getClass().getMethod("initExtractorClient"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
}
    private DeepRDD createDeepRDD() throws Exception {

//        PowerMockito.suppress(PowerMockito.constructor(DeepRDD.class, SparkContext.class,ExtractorConfig.class));
        PowerMockito.suppress(PowerMockito.constructor(DeepRDD.class, SparkContext.class,Class.class));
        return  Whitebox.newInstance(DeepRDD.class);
    }


} 
