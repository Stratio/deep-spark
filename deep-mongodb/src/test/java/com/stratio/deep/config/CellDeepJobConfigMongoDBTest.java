package com.stratio.deep.config;


import com.stratio.deep.entity.Cells;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Created by rcrespo on 16/07/14.
 */
@Test
public class CellDeepJobConfigMongoDBTest {


    @Test
    public void createTest() {

        GenericDeepJobConfigMongoDB<Cells> cellDeepJobConfigMongoDB = new CellDeepJobConfigMongoDB();

        assertNotNull(cellDeepJobConfigMongoDB);

        assertEquals(cellDeepJobConfigMongoDB.getEntityClass(), Cells.class);

    }


}
