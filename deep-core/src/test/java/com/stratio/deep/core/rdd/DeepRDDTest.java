package com.stratio.deep.core.rdd;

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.core.extractor.client.ExtractorClient;

import com.stratio.deep.core.rdd.*;
import com.stratio.deep.rdd.DeepTokenRange;
import com.stratio.deep.rdd.IDeepPartition;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import scala.collection.Iterator;
import com.stratio.deep.partition.impl.DeepPartition;
import static junit.framework.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/** 
* DeepRDD Tester. 
* 
* @author <Authors name> 
* @since <pre>ago 22, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {ExtractorClient.class, DeepPartition.class, DeepRDD.class})
public class DeepRDDTest {

    public static final Object FIRST_RESULT = "FirstResult";
    private static final Object SECOND_RESULT = "SecondResult";

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

    configureExtractorCompute(deepTokenRange, extractorConfig);
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


    /**
     *
     * Method: getPartitions()
     *
     */
    @Test
    public void testGetPartitions() throws Exception {
        DeepRDD deepRDD = createDeepRDD();

        ExtractorClient extractorClient = createExtractorClient();
        ExtractorConfig extractorConfig = mock(ExtractorConfig.class);

        DeepTokenRange deepTokenRange = mock(DeepTokenRange.class);
        DeepTokenRange otherDeepTokenRange = mock(DeepTokenRange.class);
        DeepTokenRange[] aDeepTokenRange = {deepTokenRange,otherDeepTokenRange};
        when(extractorClient.getPartitions(extractorConfig)).thenReturn(aDeepTokenRange);

        DeepPartition deepPartition = mock(DeepPartition.class);
        DeepPartition otherDeepPartition = mock(DeepPartition.class);
        whenNew(DeepPartition.class).withArguments(0, 0, deepTokenRange).thenReturn(deepPartition);
        whenNew(DeepPartition.class).withArguments(0, 1, otherDeepTokenRange).thenReturn(otherDeepPartition);

        Broadcast config = createConfig(extractorConfig);
        deepRDD.config = config;

        Partition[] partitions = deepRDD.getPartitions();

        assertNotNull("The partitions is not null", partitions);
        assertEquals("The partition length is correct",aDeepTokenRange.length,partitions.length);
        assertEquals("The first partition is correct",deepPartition,partitions[0]);
        assertEquals("The second partition is correct",otherDeepPartition,partitions[1]);

    }



    private TaskContext createTaskContext() {
        TaskContext taskcontext = mock(TaskContext.class);
        doNothing().when(taskcontext).addOnCompleteCallback(any(com.stratio.deep.core.rdd.OnComputedRDDCallback.class));
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

    private ExtractorClient configureExtractorCompute(DeepTokenRange deepTokenRange, ExtractorConfig extractorConfig ) throws Exception {
        ExtractorClient extractorClient = createExtractorClient();
        when(extractorClient.hasNext()).thenReturn(true,true,false);
        when(extractorClient.next()).thenReturn(FIRST_RESULT, SECOND_RESULT);

        DeepTokenRange[] aDeepTokenRange = {deepTokenRange};
        when(extractorClient.getPartitions(extractorConfig)).thenReturn(aDeepTokenRange);

        doNothing().when(extractorClient).initIterator(deepTokenRange, extractorConfig);

        return extractorClient;
    }

    private ExtractorClient createExtractorClient() throws Exception {
        ExtractorClient extractorClient = mock(ExtractorClient.class);
        whenNew(ExtractorClient.class).withAnyArguments().thenReturn(extractorClient);
        doNothing().when(extractorClient).initialize();
        return extractorClient;
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


        PowerMockito.suppress(PowerMockito.constructor(DeepRDD.class, SparkContext.class,Class.class));
        return  Whitebox.newInstance(DeepRDD.class);
    }


} 
