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

package com.stratio.deep.rdd;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.extractor.ExtractorTest;
import com.stratio.deep.extractor.ESCellExtractor;


import com.stratio.deep.extractor.ESEntityExtractor;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.action.count.CountResponse;
import org.testng.annotations.Test;


import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESCellRDDTest"}, dependsOnGroups = "ESJavaRDDTest")
public class ESCellRDDTest extends ExtractorTest {
    private Logger LOG = Logger.getLogger(getClass());

    public ESCellRDDTest() {
        super(ESCellExtractor.class,"localhost:9200",null,ESJavaRDDTest.ES_INDEX_MESSAGE+ESJavaRDDTest.ES_SEPARATOR+ESJavaRDDTest.ES_TYPE_MESSAGE,
                true);
    }

    @Test
    public void testReadingRDD() {
        DeepSparkContext context = null;
        try {
            // Creating the Deep Context where args are Spark Master and Job Name
            String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
            context = new DeepSparkContext("local", "deepSparkContextTest");

            // Creating a configuration for the Extractor and initialize it
            ExtractorConfig<Cell> config = new ExtractorConfig();

            config.putValue(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE);
            config.putValue(ExtractorConstants.HOST, hostConcat);

            config.setExtractorImplClass(ESCellExtractor.class);


            // Creating the RDD
            RDD<Cell> rdd = context.createRDD(config);

            assertEquals(rdd.count(), 1);


            LOG.info("-------------------------   Num of rows: " + rdd.count() + " ------------------------------");

        }finally {
            context.stop();
        }

    }

    @Test
    public void testWritingRDD() {

        // Creating the Deep Context where args are Spark Master and Job Name
        String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try{


            // Creating a configuration for the Extractor and initialize it
            ExtractorConfig<Cell> config = new ExtractorConfig();

            config.putValue(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE);
            config.putValue(ExtractorConstants.HOST, hostConcat);

            config.setExtractorImplClass(ESCellExtractor.class);

            RDD<Cell> inputRdd =  context.createRDD(config);

            long counts = inputRdd.count();
            ExtractorConfig<Cell> outputConfigEntity = new ExtractorConfig();
            outputConfigEntity.putValue(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE_OUTPUT).putValue(ExtractorConstants.HOST, hostConcat);
            outputConfigEntity.setExtractorImplClass(ESCellExtractor.class);

            //Save RDD in ES
            context.saveRDD(inputRdd, outputConfigEntity);

            RDD<Cell> outputRDDEntity = context.createRDD(outputConfigEntity);

            outputRDDEntity.count();

            CountResponse response = ESJavaRDDTest.client.prepareCount("twitter")
                    .setQuery(termQuery("_type", "tweet2")).execute().actionGet();

            LOG.info("-------------------------   Num of outputRDDEntity: " + outputRDDEntity.count() +" ------------------------------");
            LOG.info("-------------------------   Num of response count: " + response.getCount() +" ------------------------------");

            assertEquals(response.getCount(),    outputRDDEntity.count());


        }finally {
            context.stop();
        }
    }
//@Test
//public void testTransform() {
//
//
//String hostConcat = HOST.concat(":").concat(PORT.toString());
//
//ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
//IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createESDB()
//.host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();
//
//RDD<Cells> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
//try{
//inputRDDEntity.first();
//fail();
//}catch (DeepTransformException e){
//// OK
//log.info("Correctly catched DeepTransformException: " + e.getLocalizedMessage());
//}
//
//context.stop();
//
//}
}