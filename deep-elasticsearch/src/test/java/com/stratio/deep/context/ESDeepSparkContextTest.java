package com.stratio.deep.context;

import com.stratio.deep.config.CellDeepJobConfigES;
import com.stratio.deep.config.EntityDeepJobConfigES;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.ESCellRDDTest;
import com.stratio.deep.rdd.ESEntityRDDTest;
import com.stratio.deep.rdd.ESJavaRDDTest;
import com.stratio.deep.testentity.BookEntity;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Created by dgomez on 20/08/14.
 */
@Test
public class ESDeepSparkContextTest {

    private Logger log = Logger.getLogger(getClass());
/*
    @Test()
    public void ESRDDTest() {
        DeepSparkContext sc = new DeepSparkContext("local", "DeepSparkContextTest");

        RDD deepESRDD = sc.createRDD(new CellDeepJobConfigES());

        assertTrue(deepESRDD instanceof ESCellRDDTest);

        assertFalse(deepESRDD instanceof ESEntityRDDTest);


        deepESRDD = sc.createRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertTrue(deepESRDD instanceof ESEntityRDDTest);

        assertFalse(deepESRDD instanceof ESCellRDDTest);


        JavaRDD<Cells> javaRDDCells = sc.createJavaRDD(new CellDeepJobConfigES());

        assertNotNull(javaRDDCells);

        assertTrue(javaRDDCells instanceof JavaRDD);

        assertTrue(javaRDDCells instanceof ESJavaRDDTest);


        JavaRDD<BookEntity> javaRDDEntity = sc.createJavaRDD(new EntityDeepJobConfigES(BookEntity.class));

        assertNotNull(javaRDDEntity);

        assertTrue(javaRDDEntity instanceof JavaRDD);

        assertTrue(javaRDDEntity instanceof ESJavaRDDTest);


        sc.stop();


    }
    */

}
