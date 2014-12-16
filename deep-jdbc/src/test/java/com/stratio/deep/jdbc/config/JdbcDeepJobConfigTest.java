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

package com.stratio.deep.jdbc.config;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class JdbcDeepJobConfigTest {

    private static final String HOST = "localhost";
    private static final int PORT = 3306;
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";

    @Test
    public void testInitialize() throws Exception {
        JdbcDeepJobConfig deepJobConfig = new JdbcDeepJobConfig(Cells.class);
        deepJobConfig.initialize(getExtractorConfig());

        assertEquals(deepJobConfig.getHost(), HOST);
        assertEquals(deepJobConfig.getPort(), PORT);
        assertEquals(deepJobConfig.getDriverClass(), DRIVER_CLASS);
        assertEquals(deepJobConfig.getDatabase(), DATABASE);
        assertEquals(deepJobConfig.getTable(), TABLE);
        assertEquals(deepJobConfig.getJdbcUrl(), "jdbc:mysql://localhost:3306/database?");
    }

    private ExtractorConfig getExtractorConfig() {
        ExtractorConfig config = new ExtractorConfig();
        config.putValue(ExtractorConstants.HOST, HOST);
        config.putValue(ExtractorConstants.PORT, PORT);
        config.putValue(ExtractorConstants.JDBC_DRIVER_CLASS, DRIVER_CLASS);
        config.putValue(ExtractorConstants.CATALOG, DATABASE);
        config.putValue(ExtractorConstants.TABLE, TABLE);
        return config;
    }
}
