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

package com.stratio.deep.commons.config;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.Filter;

public class DeepJobConfigTest {

    private int testPort = 1234;
    private String[] testHost = new String[]{"hostTest1","hostTest2"};
    private String testCatalog = "catalog";
    private String testTable = "table";
    private String testUserName = "user";
    private String testPassword = "password";
    private String[] testInputColumns = new String[]{"field1","field2","field3"};
    private Filter[] testFilter = new Filter[]{new Filter("field1").greaterThan(10),new Filter("field1").lessThan(50)};


    @Test
    public void testInitialize() throws Exception {
        ExtractorConfig<Cells> extractorConfig = new ExtractorConfig<>(Cells.class);
        extractorConfig.putValue(ExtractorConstants.HOST, testHost);
        extractorConfig.putValue(ExtractorConstants.PORT, testPort);
        extractorConfig.putValue(ExtractorConstants.CATALOG, testCatalog);
        extractorConfig.putValue(ExtractorConstants.TABLE, testTable);
        extractorConfig.putValue(ExtractorConstants.USERNAME, testUserName);
        extractorConfig.putValue(ExtractorConstants.PASSWORD, testPassword);
        extractorConfig.putValue(ExtractorConstants.INPUT_COLUMNS, testInputColumns);
        extractorConfig.putValue(ExtractorConstants.FILTER_QUERY, testFilter);


        DeepJobConfig<Cells, DeepJobConfig> cellsDeepJobConfig = new DeepJobConfig(Cells.class);

        cellsDeepJobConfig.initialize(extractorConfig);

        assertEquals(cellsDeepJobConfig.getPort(), testPort);
        assertEquals(cellsDeepJobConfig.getHostList().toArray(new String[cellsDeepJobConfig.getHostList().size()]),
                testHost);
        assertEquals(cellsDeepJobConfig.getCatalog(), testCatalog);
        assertEquals(cellsDeepJobConfig.getTable(), testTable);
        assertEquals(cellsDeepJobConfig.getUsername(), testUserName);
        assertEquals(cellsDeepJobConfig.getPassword(), testPassword);
        assertEquals(cellsDeepJobConfig.getInputColumns(), testInputColumns);
        assertEquals(cellsDeepJobConfig.getFilters(), testFilter);



    }
}