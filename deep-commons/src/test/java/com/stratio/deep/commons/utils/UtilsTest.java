/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.commons.utils;

import static com.stratio.deep.commons.utils.Utils.removeAddressPort;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * The type Utils test.
 * <p/>
 * Created by rcrespo on 12/11/14.
 */
public class UtilsTest {

    /**
     * Test new type instance.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNewTypeInstance() throws Exception {

    }

    /**
     * Test new type instance 1.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNewTypeInstance1() throws Exception {

    }

    /**
     * Test quote.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuote() throws Exception {

    }

    /**
     * Test single quote.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSingleQuote() throws Exception {

    }

    /**
     * Test batch query generator.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBatchQueryGenerator() throws Exception {

    }

    /**
     * Test prepare tuple 4 cql driver.
     *
     * @throws Exception the exception
     */
    @Test
    public void testPrepareTuple4CqlDriver() throws Exception {

    }

    /**
     * Test find setter.
     *
     * @throws Exception the exception
     */
    @Test
    public void testFindSetter() throws Exception {

    }

    /**
     * Test find getter.
     *
     * @throws Exception the exception
     */
    @Test
    public void testFindGetter() throws Exception {

    }

    /**
     * Test inet address from location.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInetAddressFromLocation() throws Exception {

    }

    /**
     * Test get all fields.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetAllFields() throws Exception {

    }

    /**
     * Test remove address port.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRemoveAddressPort() throws Exception {
        List<String> expected = new ArrayList();
        expected.add("stratio1");
        expected.add("stratio2");
        expected.add("stratio3");
        expected.add("stratio4");

        List<String> test = new ArrayList();
        test.add("stratio1:2010");
        test.add("stratio2:2030");
        test.add("stratio3:2040");
        test.add("stratio4");

        List<String> stringList = removeAddressPort(test);

        assertEquals(stringList, expected);

    }

    /**
     * Test split list by comma.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSplitListByComma() throws Exception {

    }

    /**
     * Test get extractor instance.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetExtractorInstance() throws Exception {

    }

    /**
     * Test cast number type.
     *
     * @throws Exception the exception
     */
    @Test
    public void testCastNumberType() throws Exception {

    }
}