/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.exception;

import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 * Generic Deep exception.
 *
 *
 */
@Test
public class DeepGenericExceptionTest  {

    private final String    message = "message";
    private final Throwable throwsmessage = new Throwable(message);


    @Test
    public void deepGenericExceptionTest(){



        try {
            throw new DeepGenericException(message);


        } catch (DeepGenericException e) {
            Assert.assertEquals(e.getMessage(), message);
        }

    }

    @Test
    public void deepGenericExceptionTest2(){



        try {
            throw new DeepGenericException(message,throwsmessage);


        } catch (DeepGenericException e) {
            Assert.assertEquals(e.getMessage(), message);
            Assert.assertEquals(e.getCause(), throwsmessage);
        }

    }
}
