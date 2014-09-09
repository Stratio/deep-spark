package com.stratio.deep.elasticsearch.test;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.extractor.ESEntityExtractor;

import com.stratio.deep.rdd.ESJavaRDDTest;
import com.stratio.deep.testentity.BookEntity;

import com.stratio.deep.testentity.TweetES;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by dgomez on 28/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"RecoverEntities"}, dependsOnGroups = "ESJavaRDDTest")
public class RecoverEntities {

    private static final Logger LOG = Logger.getLogger(RecoverEntities.class);

    /* used to perform external tests */

    private String job = "java:aggregatingData";

    private String KEYSPACENAME = "test";
    private String TABLENAME    = "tweets";
    private String PORT         = "9200";
    private String HOST         = "127.0.0.1"+PORT;

    /**
     *
     *
     *
     */


    /**
     * This is the method called by both main and tests.
     *
     */
    @Test
    public void recoverEntity() {


        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(null);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());
        try{
            // Creating a configuration for the Extractor and initialize it
            ExtractorConfig<TweetES> config = new ExtractorConfig(TweetES.class);

            config.setExtractorImplClass(ESEntityExtractor.class);
            config.setEntityClass(TweetES.class);

            Map<String, String> values = new HashMap<>();

            values.put(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE);
            values.put(ExtractorConstants.HOST,     ESJavaRDDTest.HOST );

            config.setValues(values);

            // Creating the RDD
            RDD<TweetES> rdd = deepContext.createRDD(config);
            LOG.info("count: " + rdd.count());
            LOG.info("first: " + rdd.first());

        }finally {
            //close
            //ExtractorServer.close();
            deepContext.stop();
        }
    }


}
