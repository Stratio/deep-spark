package com.stratio.deep.entity;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by rcrespo on 16/07/14.
 */
@Test
public class ESCellTest {

    @Test
    public void createTest() {


        Cell cell1 = ESCell.create("cellName1", "testStringObject");


        assertEquals(cell1.getCellName(), "cellName1");

        assertEquals(cell1.getCellValue().getClass(), String.class);

    }

//    @Test
//    public void isKeyTest() {
//
//
//        Cell cell2 = ESCell.create("_id", "_idObject");
//
//
//        assertTrue(cell2.isKey());
//
//    }

}
