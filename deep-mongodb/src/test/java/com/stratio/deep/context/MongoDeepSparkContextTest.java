package com.stratio.deep.context;

import com.stratio.deep.config.CellDeepJobConfigMongoDB;
import com.stratio.deep.config.EntityDeepJobConfigMongoDB;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.mongodb.MongoCellRDD;
import com.stratio.deep.rdd.mongodb.MongoEntityRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.testentity.BookEntity;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.DeepMongoRDD;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;


/**
 * Created by rcrespo on 16/07/14.
 */
@Test
public class MongoDeepSparkContextTest {

    private Logger log = Logger.getLogger(getClass());

    @Test()
    public void mongoRDDTest() {
        MongoDeepSparkContext sc = new MongoDeepSparkContext("local", "MongoDeepSparkContextTest");

        DeepMongoRDD deepMongoRDD = sc.mongoRDD(new CellDeepJobConfigMongoDB());

        assertTrue(deepMongoRDD instanceof MongoCellRDD);

        assertFalse(deepMongoRDD instanceof MongoEntityRDD);


        deepMongoRDD = sc.mongoRDD(new EntityDeepJobConfigMongoDB(BookEntity.class));

        assertTrue(deepMongoRDD instanceof MongoEntityRDD);

        assertFalse(deepMongoRDD instanceof MongoCellRDD);


        JavaRDD<Cells> javaRDDCells = sc.mongoJavaRDD(new CellDeepJobConfigMongoDB());

        assertNotNull(javaRDDCells);

        assertTrue(javaRDDCells instanceof JavaRDD);

        assertTrue(javaRDDCells instanceof MongoJavaRDD);


        JavaRDD<BookEntity> javaRDDEntity = sc.mongoJavaRDD(new EntityDeepJobConfigMongoDB(BookEntity.class));

        assertNotNull(javaRDDEntity);

        assertTrue(javaRDDEntity instanceof JavaRDD);

        assertTrue(javaRDDEntity instanceof MongoJavaRDD);

        try {
            DeepMongoRDD failRDD = sc.mongoRDD(new GenericDeepJobConfigMongoDB());
            fail();
        } catch (DeepGenericException e) {
            log.info("Correctly catched DeepGenericException: " + e.getLocalizedMessage());
        }


        sc.stop();


    }

    @Test
    public void testInstantiationBySparkContext() {
        MongoDeepSparkContext sc = new MongoDeepSparkContext(new SparkContext("local", "myapp1", new SparkConf()));

        assertNotNull(sc);

        DeepMongoRDD deepMongoRDD = sc.mongoRDD(new CellDeepJobConfigMongoDB());

        assertTrue(deepMongoRDD instanceof MongoCellRDD);

        assertFalse(deepMongoRDD instanceof MongoEntityRDD);


        deepMongoRDD = sc.mongoRDD(new EntityDeepJobConfigMongoDB(BookEntity.class));

        assertTrue(deepMongoRDD instanceof MongoEntityRDD);

        assertFalse(deepMongoRDD instanceof MongoCellRDD);

        sc.stop();
    }

    @Test
    public void testInstantiationWithJar() {
        MongoDeepSparkContext sc = new MongoDeepSparkContext("local", "myapp1", "/tmp", "");
        assertNotNull(sc);

        DeepMongoRDD deepMongoRDD = sc.mongoRDD(new CellDeepJobConfigMongoDB());

        assertTrue(deepMongoRDD instanceof MongoCellRDD);

        assertFalse(deepMongoRDD instanceof MongoEntityRDD);


        deepMongoRDD = sc.mongoRDD(new EntityDeepJobConfigMongoDB(BookEntity.class));

        assertTrue(deepMongoRDD instanceof MongoEntityRDD);

        assertFalse(deepMongoRDD instanceof MongoCellRDD);

        sc.stop();
    }

    @Test
    public void testInstantiationWithJars() {
        MongoDeepSparkContext sc = new MongoDeepSparkContext("local", "myapp1", "/tmp", new String[]{"", ""});
        assertNotNull(sc);

        DeepMongoRDD deepMongoRDD = sc.mongoRDD(new CellDeepJobConfigMongoDB());

        assertTrue(deepMongoRDD instanceof MongoCellRDD);

        assertFalse(deepMongoRDD instanceof MongoEntityRDD);


        deepMongoRDD = sc.mongoRDD(new EntityDeepJobConfigMongoDB(BookEntity.class));

        assertTrue(deepMongoRDD instanceof MongoEntityRDD);

        assertFalse(deepMongoRDD instanceof MongoCellRDD);

        sc.stop();
    }

    @Test
    public void testInstantiationWithJarsAndEnv() {
        Map<String, String> env = new HashMap<>();

        MongoDeepSparkContext sc = new MongoDeepSparkContext("local", "myapp1", "/tmp", new String[]{"", ""}, env);

        assertNotNull(sc);
        DeepMongoRDD deepMongoRDD = sc.mongoRDD(new CellDeepJobConfigMongoDB());

        assertTrue(deepMongoRDD instanceof MongoCellRDD);

        assertFalse(deepMongoRDD instanceof MongoEntityRDD);


        deepMongoRDD = sc.mongoRDD(new EntityDeepJobConfigMongoDB(BookEntity.class));

        assertTrue(deepMongoRDD instanceof MongoEntityRDD);

        assertFalse(deepMongoRDD instanceof MongoCellRDD);


        sc.stop();
    }
}
