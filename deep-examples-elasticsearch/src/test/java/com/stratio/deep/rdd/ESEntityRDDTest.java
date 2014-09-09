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
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.extractor.ESEntityExtractor;
import com.stratio.deep.testentity.MessageTestEntity;
import com.stratio.deep.testentity.TweetES;
import com.stratio.deep.testentity.WordCount;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESEntityRDDTest"}, dependsOnGroups = "ESJavaRDDTest")
public class ESEntityRDDTest implements Serializable {

    private static final Logger LOG = Logger.getLogger(ESEntityRDDTest.class);

    @Test
    public void testReadingRDD() {
        DeepSparkContext context = null;
        try {
            // Creating the Deep Context where args are Spark Master and Job Name
            String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
            context = new DeepSparkContext("local", "deepSparkContextTest");

            // Creating a configuration for the Extractor and initialize it
            ExtractorConfig<TweetES> config = new ExtractorConfig(TweetES.class);

            config.putValue(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE);
            config.putValue(ExtractorConstants.HOST, hostConcat);

            config.setExtractorImplClass(ESEntityExtractor.class);
            config.setEntityClass(TweetES.class);


            // Creating the RDD
            RDD<TweetES> rdd = context.createRDD(config);

            assertEquals(rdd.count(), 1);

            TweetES[] collection = (TweetES[]) rdd.collect();
            LOG.info("-------------------------   Num of rows: " + rdd.count() + " ------------------------------");
            LOG.info("-------------------------   Num of Columns: " + collection.length + " ------------------------------");
            LOG.info("-------------------------   Element Message: " + collection[0].getMessage() + " ------------------------------");
        }catch (Exception e){
            System.out.printf(" E------------------ "+e);
            LOG.error(e.getMessage());
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
            ExtractorConfig<TweetES> config = new ExtractorConfig(TweetES.class);

            config.putValue(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE);
            config.putValue(ExtractorConstants.HOST, hostConcat);

            config.setExtractorImplClass(ESEntityExtractor.class);
            config.setEntityClass(TweetES.class);

            RDD<TweetES> inputRdd =  context.createRDD(config);

            long counts = inputRdd.count();
            ExtractorConfig<TweetES> outputConfigEntity = new ExtractorConfig(TweetES.class);
            outputConfigEntity.putValue(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE).putValue(ExtractorConstants.HOST, hostConcat);
            outputConfigEntity.setExtractorImplClass(ESEntityExtractor.class);

            //Save RDD in ES
            context.saveRDD(inputRdd, outputConfigEntity);

            RDD<TweetES> outputRDDEntity = context.createRDD(outputConfigEntity);

            outputRDDEntity.count();

            CountResponse response = ESJavaRDDTest.client.prepareCount("twitter")
                    .setQuery(termQuery("_type", "tweet")).execute().actionGet();

            LOG.info("-------------------------   Num of outputRDDEntity: " + outputRDDEntity.count() +" ------------------------------");
            LOG.info("-------------------------   Num of response count: " + response.getCount() +" ------------------------------");

            assertEquals(response.getCount(),    outputRDDEntity.count());

        }catch (Exception e){
            System.out.printf(" E------------------ "+e);
            LOG.error(e.getMessage());

        }finally {
            context.stop();
        }
    }



    public void testDataSet() {


    }




}