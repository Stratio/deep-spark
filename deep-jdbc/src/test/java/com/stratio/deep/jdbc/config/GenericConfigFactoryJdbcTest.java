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

import static org.testng.Assert.fail;

import com.mysql.jdbc.Driver;
import com.stratio.deep.commons.entity.Cells;
import org.testng.annotations.Test;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class GenericConfigFactoryJdbcTest {

    private static final String HOST = "localhost";

    private static final int PORT = 3306;

    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private static final String DATABASE = "test";

    private static final String TABLE = "table";

    private static final Integer ONE_PARTITION = 1;

    private static final Integer TWO_PARTITIONS = 2;

    private static final String PARTITION_KEY = "rowid";

    @Test
    public void testDriverClassValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).port(PORT).database(DATABASE).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.driverClass(DRIVER_CLASS);
        }
        config.initialize();
    }

    @Test
    public void testTableValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).port(PORT).driverClass(DRIVER_CLASS).database(DATABASE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.table(TABLE);
        }
        config.initialize();
    }

    @Test
    public void testHostPortJdbcUrlValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.port(PORT).driverClass(DRIVER_CLASS).database(DATABASE).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.connectionUrl("testUrl");
        }
        config.initialize();
    }

    @Test
    public void testPortValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).driverClass(DRIVER_CLASS).database(DATABASE).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.port(PORT);
        }
        config.initialize();
    }

    @Test
    public void testPartitionKeyValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).driverClass(DRIVER_CLASS).database(DATABASE).table(TABLE).port(PORT).numPartitions(TWO_PARTITIONS);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.partitionKey(PARTITION_KEY);
        }
        config.initialize();
    }

    @Test
    public void testPartitionKeyValidationNotRequired() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).driverClass(DRIVER_CLASS).database(DATABASE).table(TABLE).port(PORT).numPartitions(ONE_PARTITION);
        try {
            config.initialize();
        } catch(IllegalArgumentException e) {
            fail();
        }
    }

}
