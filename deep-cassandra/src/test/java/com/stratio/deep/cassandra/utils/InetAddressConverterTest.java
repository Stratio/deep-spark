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

package com.stratio.deep.cassandra.utils;

import com.stratio.deep.cassandra.util.InetAddressConverter;

import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;
import static org.testng.Assert.*;

/**
 * Created by luca on 11/07/14.
 */
public class InetAddressConverterTest {


    @Test
    public void testOneAddress() {
        Collection<InetAddress> returnValue = InetAddressConverter.toInetAddress("127.0.0.1");
        assertEquals(1, returnValue.size());
        assertEquals("localhost",returnValue.toArray(new InetAddress[0])[0].getCanonicalHostName());
    }

    @Test
    public void testManyAddress() {
        Collection<InetAddress> returnValue = InetAddressConverter.toInetAddress("127.0.0.1,10.0.12.13");
        assertEquals(2, returnValue.size());
        InetAddress[] inetAddressesArray = returnValue.toArray(new InetAddress[0]);
        assertEquals("localhost", inetAddressesArray[0].getCanonicalHostName());
        assertEquals("10.0.12.13", inetAddressesArray[1].getCanonicalHostName());
    }


}
