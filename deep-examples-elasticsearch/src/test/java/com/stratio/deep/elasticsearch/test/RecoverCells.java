package com.stratio.deep.elasticsearch.test;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.extractor.ESCellExtractor;

import com.stratio.deep.rdd.ESJavaRDDTest;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by dgomez on 28/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"RecoverCells"}, dependsOnGroups = "ESJavaRDDTest")
public class RecoverCells {

    private static final Logger LOG = Logger.getLogger(RecoverCells.class);

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
    public void recoverCells() {


        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(null);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        try{
            // Creating a configuration for the Extractor and initialize it
            ExtractorConfig<Cells> config = new ExtractorConfig(Cells.class);

            config.setExtractorImplClass(ESCellExtractor.class);
            config.setEntityClass(Cells.class);

            Map<String, String> values = new HashMap<>();

            values.put(ExtractorConstants.DATABASE, ESJavaRDDTest.DATABASE);
            values.put(ExtractorConstants.HOST,     ESJavaRDDTest.HOST );

            config.setValues(values);


            // Creating the RDD
            RDD<Cells> rdd = deepContext.createRDD(config);
            LOG.info("count: " + rdd.count());
            LOG.info("first: " + rdd.first());

        }finally {
            //close

            deepContext.stop();
        }

    }


}
