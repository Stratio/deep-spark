package com.stratio.deep.exception;

import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.FileAssert.fail;

/**
 * Created by dgomez on 21/08/14.
 */
public class ExceptionTest {

    final String message = "message";

    @Test
    public void testExceptionMessage() {
        try {
            new ArrayList<Object>().get(0);
            fail("Expected an IndexOutOfBoundsException to be thrown");

        } catch (IndexOutOfBoundsException anIndexOutOfBoundsException) {
            Assert.assertEquals(anIndexOutOfBoundsException.getMessage(), "Index: 0, Size: 0");
        }
    }

    @Test(expectedExceptions = { DeepGenericException.class })
    public void deepGenericExceptionTest(){
        try {
            throw new DeepGenericException(message);


        } catch (DeepGenericException e) {
            Assert.assertEquals(e.getMessage(), message);
        }

    }

}
