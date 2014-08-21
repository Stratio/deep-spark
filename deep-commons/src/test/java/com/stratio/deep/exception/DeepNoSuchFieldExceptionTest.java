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
 *
 *
 */

@Test
public class DeepNoSuchFieldExceptionTest {
    private final String    message = "message";
    private final Throwable throwsmessage = new Throwable(message);


    @Test(expectedExceptions = { DeepNoSuchFieldException.class })
    public void deepNoSuchFieldExceptionTest(){



        try {
            throw new DeepNoSuchFieldException(message);


        } catch (DeepNoSuchFieldException e) {
            Assert.assertEquals(e.getMessage(), message);
        }
        throw new DeepNoSuchFieldException(message,throwsmessage);

    }

    @Test(expectedExceptions = { DeepNoSuchFieldException.class })
    public void deepNoSuchFieldExceptionTest2(){



        try {
            throw new DeepNoSuchFieldException(message,throwsmessage);


        } catch (DeepNoSuchFieldException e) {
            Assert.assertEquals(e.getMessage(), message);
            Assert.assertEquals(e.getCause(), throwsmessage);
        }
        throw new DeepNoSuchFieldException(message,throwsmessage);

    }


}
