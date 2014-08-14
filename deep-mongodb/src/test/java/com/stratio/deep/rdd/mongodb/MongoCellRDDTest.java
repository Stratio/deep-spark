package com.stratio.deep.rdd.mongodb;

import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.config.MongoConfigFactory;
import com.stratio.deep.context.MongoDeepSparkContext;
import com.stratio.deep.entity.Cells;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import static com.stratio.deep.rdd.mongodb.MongoJavaRDDTest.*;
import static org.testng.Assert.assertEquals;

/**
 * Created by rcrespo on 16/07/14.
 */
@Test(suiteName = "mongoRddTests", groups = {"MongoCellRDDTest"})
public class MongoCellRDDTest {

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testReadingRDD() {

        String hostConcat = MongoJavaRDDTest.HOST.concat(":").concat(MongoJavaRDDTest.PORT.toString());
        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<Cells> inputConfigEntity = MongoConfigFactory.createMongoDB()
                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();

        JavaRDD<Cells> inputRDDEntity = context.mongoJavaRDD(inputConfigEntity);

        assertEquals(MongoJavaRDDTest.col.count(), inputRDDEntity.cache().count());
        assertEquals(MongoJavaRDDTest.col.findOne().get("message"), inputRDDEntity.first().getCellByName("message").getCellValue());

        context.stop();

    }

    @Test
    public void testWritingRDD() {


        String hostConcat = HOST.concat(":").concat(PORT.toString());

        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<Cells> inputConfigEntity = MongoConfigFactory.createMongoDB()
                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();

        RDD<Cells> inputRDDEntity = context.mongoRDD(inputConfigEntity);

        IMongoDeepJobConfig<Cells> outputConfigEntity = MongoConfigFactory.createMongoDB()
                .host(hostConcat).database(DATABASE).collection(COLLECTION_OUTPUT_CELL).initialize();


        //Save RDD in MongoDB
        MongoCellRDD.saveCell(inputRDDEntity, outputConfigEntity);

        RDD<Cells> outputRDDEntity = context.mongoRDD(outputConfigEntity);


        assertEquals(MongoJavaRDDTest.mongo.getDB(DATABASE).getCollection(COLLECTION_OUTPUT_CELL).findOne().get("message"),
                outputRDDEntity.first().getCellByName("message").getCellValue());


        context.stop();


    }

//    @Test
//    public void testTransform() {
//
//
//        String hostConcat = HOST.concat(":").concat(PORT.toString());
//
//        MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");
//
//        IMongoDeepJobConfig<Cells> inputConfigEntity = MongoConfigFactory.createMongoDB()
//                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();
//
//        RDD<Cells> inputRDDEntity = context.mongoRDD(inputConfigEntity);
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
