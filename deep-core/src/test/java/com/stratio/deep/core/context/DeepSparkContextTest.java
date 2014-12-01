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

package com.stratio.deep.core.context;

import static junit.framework.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.Serializable;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.rdd.DeepJavaRDD;
import com.stratio.deep.core.rdd.DeepRDD;

/**
 * Tests DeepSparkContext instantiations.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { SparkContext.class, DeepJavaRDD.class, DeepSparkContext.class, DeepRDD.class, Method.class,
        AccessibleObject.class, System.class })
public class DeepSparkContextTest {


    DeepSparkContext deepSparkContext;

    @Mock
    SparkContext sparkContext;

    @Mock
    private JavaRDD<Cells> singleRdd;

    @Test
    public void createRDDTest() throws Exception {
        DeepSparkContext deepSparkContext = createDeepSparkContext();
        ExtractorConfig deepJobConfig = createDeepJobConfig();
        DeepRDD deepRDD = createDeepRDD(deepSparkContext.sc(), deepJobConfig);

        DeepRDD rddReturn = (DeepRDD) deepSparkContext.createRDD(deepJobConfig);

        assertSame("The DeepRDD is the same", deepRDD, rddReturn);

    }

    @Test
    public void createHDFSRDDTest() throws Exception {


        deepSparkContext= new DeepSparkContext(sparkContext);
        Whitebox.setInternalState(deepSparkContext,"sc",sparkContext);
        RDD<String> rdd = mock(RDD.class);
        JavaRDD<String> javaRdd = mock(JavaRDD.class);
        when(deepSparkContext.sc().textFile(anyString(), anyInt())).thenReturn(rdd);

        when(rdd.toJavaRDD()).thenReturn(javaRdd);
        when(rdd.toJavaRDD().map(any(Function.class))).thenReturn(singleRdd);

        ExtractorConfig<Cells> config = createHDFSDeepJobConfig();

        JavaRDD rddReturn = deepSparkContext.createHDFSRDD(config);

        verify(deepSparkContext.sc(), times(1)).textFile(anyString(),anyInt());

        verify(javaRdd,times(1)).map(any(Function.class));

    }

    @Test
    public void JavaRDDTest() throws Exception {
        DeepSparkContext deepSparkContext = createDeepSparkContext();
        ExtractorConfig deepJobConfig = createDeepJobConfig();
        DeepRDD deepRDD = createDeepRDD(deepSparkContext.sc(), deepJobConfig);
        DeepJavaRDD javaRdd = createDeepJAvaRDD(deepRDD);

        JavaRDD javaRDdReturn = deepSparkContext.createJavaRDD(deepJobConfig);

        assertSame("The DeepJavaRDD is Same", javaRdd, javaRDdReturn);
    }

    @Test
    public void saveRDDTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

      /*  DeepSparkContext deepSparkContext = createDeepSparkContext();

        IDeepJobConfig<Cell,  IDeepJobConfig<?, ?>> ideepJobConfig = mock(IDeepJobConfig.class);
        RDD<Cell> rdd = mock(RDD.class);
        Method method = new Method(){
            public Object invoke(Object obj, Object... args){

                   return null;
            }
        };

        Collection aa = PowerMockito.mock(Collection.class);
        when(ideepJobConfig.getSaveMethod()).thenReturn(method);

        deepSparkContext.saveRDD(rdd,ideepJobConfig);

        Mockito.verify(method).invoke(null, rdd, ideepJobConfig);*/
    }

    private DeepJavaRDD createDeepJAvaRDD(DeepRDD deepRDD) throws Exception {
        DeepJavaRDD javaRdd = mock(DeepJavaRDD.class);
        whenNew(DeepJavaRDD.class).withArguments(deepRDD).thenReturn(javaRdd);
        return javaRdd;
    }

    private DeepSparkContext createDeepSparkContext() {
        PowerMockito.suppress(PowerMockito.constructor(DeepSparkContext.class, SparkContext.class));
        return Whitebox.newInstance(DeepSparkContext.class);
    }

    private ExtractorConfig createDeepJobConfig() {
        ExtractorConfig extractorConfig = mock(ExtractorConfig.class);
        when(extractorConfig.getExtractorImplClass()).thenReturn(new Object().getClass());
        //when(extractorConfig.getInputFormatClass()).thenReturn(null);
        return extractorConfig;
    }

    private ExtractorConfig createHDFSDeepJobConfig() {

        ExtractorConfig extractorConfig = new ExtractorConfig();
        extractorConfig.setExtractorImplClassName("hdfs");
        Map<String, Serializable> values = new HashMap<>();
        values.put(ExtractorConstants.PORT, "9000");
        values.put(ExtractorConstants.HDFS_FILE_SEPARATOR, ",");
        values.put(ExtractorConstants.HDFS_FILE_PATH, "/user/hadoop/test/songs.csv");
        values.put(ExtractorConstants.HOST, "127.0.0.1");

        values.put(ExtractorConstants.HDFS_TYPE,ExtractorConstants.HDFS_TYPE);
        values.put(ExtractorConstants.TABLE,"");
        values.put(ExtractorConstants.CATALOG,"");

        extractorConfig.setValues(values);

        return extractorConfig;
    }


    private DeepRDD createDeepRDD(SparkContext sc, ExtractorConfig deepJobConfig) throws Exception {

        DeepRDD deepRDD = mock(DeepRDD.class);

        whenNew(DeepRDD.class).withArguments(sc, deepJobConfig).thenReturn(deepRDD);
        return deepRDD;
    }


}
