package com.stratio.deep.entity;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.mongodb.entity.MongoCell;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by rcrespo on 16/07/14.
 */
@Test
public class MongoCellTest {

    @Test
    public void createTest() {


        Cell cell1 = MongoCell.create("cellName1", "testStringObject");


        assertEquals(cell1.getCellName(), "cellName1");

        assertEquals(cell1.getCellValue().getClass(), String.class);

    }

    @Test
    public void isKeyTest() {


        Cell cell2 = MongoCell.create("_id", "_idObject");


        //assertTrue(cell2.isKey());

    }

}
