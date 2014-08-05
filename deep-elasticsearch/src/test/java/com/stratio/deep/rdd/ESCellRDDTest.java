package com.stratio.deep.rdd;

import com.stratio.deep.config.IESDeepJobConfig;
import com.stratio.deep.config.ESConfigFactory;
import com.stratio.deep.context.ESDeepSparkContext;
import com.stratio.deep.entity.Cells;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;


import static org.testng.Assert.assertEquals;
import static com.stratio.deep.rdd.ESJavaRDDTest.*;
/**
 * Created by rcrespo on 16/07/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESCellRDDTest"})
public class ESCellRDDTest {

    private Logger log = Logger.getLogger(getClass());

//    @Test
//    public void testReadingRDD() {
//
//        String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
//        ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
//        IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createES()
//                .host(hostConcat).database(DATABASE).initialize();
//
//        RDD<Cells> inputRDDEntity = context.ESRDD (inputConfigEntity);
//
//
////        assertEquals(0, inputRDDEntity.cache().count());
////        assertEquals(ESJavaRDDTest.col.findOne().get("message"), inputRDDEntity.first().getCellByName("message").getCellValue());
//
//        context.stop();
//
//    }
//
//    @Test
//    public void testWritingRDD() {
//
//
//        String hostConcat = HOST.concat(":").concat(PORT.toString());
//
//        ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
//        IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createES()
//                .host(hostConcat).database(DATABASE).initialize();
//
//        RDD<Cells> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
//        IESDeepJobConfig<Cells> outputConfigEntity = ESConfigFactory.createES()
//                .host(hostConcat).database(DATABASE_OUTPUT).initialize();
//
//
//        //Save RDD in ESDB
//        ESCellRDD.saveCell(inputRDDEntity, outputConfigEntity);
//
//        RDD<Cells> outputRDDEntity = context.ESRDD(outputConfigEntity);
//
//
//        assertEquals(ESJavaRDDTest. ES.getDB(DATABASE).getCollection(COLLECTION_OUTPUT_CELL).findOne().get("message"),
//                outputRDDEntity.first().getCellByName("message").getCellValue());
//
//
//        context.stop();
//

//    }

//    @Test
//    public void testTransform() {
//
//
//        String hostConcat = HOST.concat(":").concat(PORT.toString());
//
//        ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
//        IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createESDB()
//                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();
//
//        RDD<Cells> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
//        try{
//            inputRDDEntity.first();
//            fail();
//        }catch (DeepTransformException e){
//            // OK
//            log.info("Correctly catched DeepTransformException: " + e.getLocalizedMessage());
//        }
//
//        context.stop();
//
//    }

}
