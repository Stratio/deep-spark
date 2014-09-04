package com.stratio.deep.rdd;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.extractor.MongoCellExtractor;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import static com.stratio.deep.rdd.MongoJavaRDDTest.*;
import static org.testng.Assert.assertEquals;

/**
 * Created by rcrespo on 16/07/14.
 */
@Test(suiteName = "mongoRddTests", groups = {"MongoCellRDDTest"}, dependsOnGroups = "MongoJavaRDDTest" )
//@Test
public class MongoCellRDDTest {

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testReadingRDD() {

        String hostConcat = MongoJavaRDDTest.HOST.concat(":").concat(MongoJavaRDDTest.PORT.toString());
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        ExtractorConfig<Cells> inputConfigEntity = new ExtractorConfig();
        inputConfigEntity.putValue(ExtractorConstants.HOST,hostConcat).putValue(ExtractorConstants.DATABASE, DATABASE).putValue(ExtractorConstants.COLLECTION,COLLECTION_INPUT);
        inputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);


        JavaRDD<Cells> inputRDDEntity = context.createJavaRDD(inputConfigEntity);

        assertEquals(MongoJavaRDDTest.col.count(), inputRDDEntity.cache().count());
        assertEquals(MongoJavaRDDTest.col.findOne().get("message"), inputRDDEntity.first().getCellByName("message").getCellValue());

        context.stop();

    }



    @Test
    public void testWritingRDD() {


        String hostConcat = HOST.concat(":").concat(PORT.toString());

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        ExtractorConfig<Cells> inputConfigEntity = new ExtractorConfig();
        inputConfigEntity.putValue(ExtractorConstants.HOST, hostConcat).putValue(ExtractorConstants.DATABASE, DATABASE).putValue(ExtractorConstants.COLLECTION, COLLECTION_INPUT);
        inputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);

        RDD<Cells> inputRDDEntity = context.createRDD(inputConfigEntity);

        ExtractorConfig<Cells> outputConfigEntity = new ExtractorConfig();
        outputConfigEntity.putValue(ExtractorConstants.HOST,hostConcat).putValue(ExtractorConstants.DATABASE, DATABASE).putValue(ExtractorConstants.COLLECTION,COLLECTION_OUTPUT_CELL);
        outputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);


        //Save RDD in MongoDB
        context.saveRDD(inputRDDEntity, outputConfigEntity);


        RDD<Cells> outputRDDEntity = context.createRDD(outputConfigEntity);


        assertEquals(MongoJavaRDDTest.mongo.getDB(DATABASE).getCollection(COLLECTION_OUTPUT_CELL).findOne().get("message"),
                outputRDDEntity.first().getCellByName("message").getCellValue());


        context.stop();


    }



}
