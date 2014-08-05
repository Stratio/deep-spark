package com.stratio.deep.context;


import com.stratio.deep.config.CellDeepJobConfigES;
import com.stratio.deep.config.EntityDeepJobConfigES;
import com.stratio.deep.config.GenericDeepJobConfigES;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;

import com.stratio.deep.rdd.ESCellRDD;
import com.stratio.deep.rdd.ESEntityRDD;
import com.stratio.deep.rdd.ESJavaRDD;
import com.stratio.deep.testentity.BookEntity;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;


/**
 * Created by rcrespo on 16/07/14.
 */
@Test
public class ESDeepSparkContextTest {

    private Logger log = Logger.getLogger(getClass());

    @Test()
    public void ESRDDTest() {
        ESDeepSparkContext sc = new ESDeepSparkContext("local", "ESDeepSparkContextTest");

        RDD deepESRDD = sc.ESRDD(new CellDeepJobConfigES());

        assertTrue(deepESRDD instanceof ESCellRDD);

        assertFalse(deepESRDD instanceof ESEntityRDD);


        deepESRDD = sc.ESRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertTrue(deepESRDD instanceof ESEntityRDD);

        assertFalse(deepESRDD instanceof ESCellRDD);


        JavaRDD<Cells> javaRDDCells = sc.elasticJavaRDD(new CellDeepJobConfigES());

        assertNotNull(javaRDDCells);

        assertTrue(javaRDDCells instanceof JavaRDD);

        assertTrue(javaRDDCells instanceof ESJavaRDD);


        JavaRDD<BookEntity> javaRDDEntity = sc.elasticJavaRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertNotNull(javaRDDEntity);

        assertTrue(javaRDDEntity instanceof JavaRDD);

        assertTrue(javaRDDEntity instanceof ESJavaRDD);



        sc.stop();


    }

    @Test
    public void testInstantiationBySparkContext() {
        ESDeepSparkContext sc = new ESDeepSparkContext(new SparkContext("local", "myapp1", new SparkConf()));

        assertNotNull(sc);

        RDD deepESRDD = sc.ESRDD(new CellDeepJobConfigES());

        assertTrue(deepESRDD instanceof ESCellRDD);

        assertFalse(deepESRDD instanceof ESEntityRDD);


        deepESRDD = sc.ESRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertTrue(deepESRDD instanceof ESEntityRDD);

        assertFalse(deepESRDD instanceof ESCellRDD);

        sc.stop();
    }

    @Test
    public void testInstantiationWithJar() {
        ESDeepSparkContext sc = new ESDeepSparkContext("local", "myapp1", "/tmp", "");
        assertNotNull(sc);

        RDD deepESRDD = sc.ESRDD(new CellDeepJobConfigES());

        assertTrue(deepESRDD instanceof ESCellRDD);

        assertFalse(deepESRDD instanceof ESEntityRDD);


        deepESRDD = sc.ESRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertTrue(deepESRDD instanceof ESEntityRDD);

        assertFalse(deepESRDD instanceof ESCellRDD);

        sc.stop();
    }

    @Test
    public void testInstantiationWithJars() {
        ESDeepSparkContext sc = new ESDeepSparkContext("local", "myapp1", "/tmp", new String[]{"", ""});
        assertNotNull(sc);

        RDD deepESRDD = sc.ESRDD(new CellDeepJobConfigES());

        assertTrue(deepESRDD instanceof ESCellRDD);

        assertFalse(deepESRDD instanceof ESEntityRDD);


        deepESRDD = sc.ESRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertTrue(deepESRDD instanceof ESEntityRDD);

        assertFalse(deepESRDD instanceof ESCellRDD);

        sc.stop();
    }

    @Test
    public void testInstantiationWithJarsAndEnv() {
        Map<String, String> env = new HashMap<>();

        ESDeepSparkContext sc = new ESDeepSparkContext("local", "myapp1", "/tmp", new String[]{"", ""}, env);

        assertNotNull(sc);
        RDD deepESRDD = sc.ESRDD(new CellDeepJobConfigES());

        assertTrue(deepESRDD instanceof ESCellRDD);

        assertFalse(deepESRDD instanceof ESEntityRDD);


        deepESRDD = sc.ESRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertTrue(deepESRDD instanceof ESEntityRDD);

        assertFalse(deepESRDD instanceof ESCellRDD);


        sc.stop();
    }
}
