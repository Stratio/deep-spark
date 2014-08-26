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

package com.stratio.deep.config;

import java.lang.annotation.AnnotationTypeMismatchException;
import java.util.HashMap;
import java.util.Map;

import com.stratio.deep.context.AbstractDeepSparkContextTest;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.extractor.utils.ExtractorConstants;
import com.stratio.deep.rdd.CassandraCellExtractor;
import com.stratio.deep.testentity.TestEntity;
import com.stratio.deep.testentity.WronglyMappedTestEntity;
import com.stratio.deep.utils.Constants;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

@Test(suiteName = "cassandraRddTests", groups = {"GenericDeepJobConfigTest"},
        dependsOnGroups = {"CassandraJavaRDDTest"})
public class GenericConfigFactoryTest extends AbstractDeepSparkContextTest {
    class NotAnnotatedTestEntity implements IDeepType {
        private static final long serialVersionUID = -2603126590709315326L;
    }

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testWriteConfigValidation() {
        ExtractorConfig<TestEntity> djc = new ExtractorConfig(TestEntity.class);


        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, "inexistent_test_page");
        values.put(ExtractorConstants.CQLPORT,  CassandraServer.CASSANDRA_CQL_PORT);
        values.put(ExtractorConstants.RPCPORT,  CassandraServer.CASSANDRA_THRIFT_PORT);

        djc.setValues(values);
        djc.rpcPort().cqlPort()
                .columnFamily("inexistent_test_page").keyspace(KEYSPACE_NAME);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.createTableOnWrite(true);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.initialize();
    }

    @Test
    public void testInputColumnsExist() {
        DeepJobConfig<Cells> djc = CassandraConfigFactory.create();

        djc.rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT).cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                .columnFamily(COLUMN_FAMILY).keyspace(KEYSPACE_NAME).inputColumns("not_existent_col1",
                "not_existent_col2");

        try {
            djc.initialize();
            fail();
        } catch (DeepNoSuchFieldException iae) {
            // OK
            log.info("Correctly catched DeepNoSuchFieldException: " + iae.getLocalizedMessage());
            djc.inputColumns("domain_name", "response_time", "url");
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.initialize();
    }

    @Test
    public void testValidation() {

        DeepJobConfig<TestEntity> djc = CassandraConfigFactory.create(TestEntity.class);

        djc.host(null).rpcPort(null).pageSize(0).bisectFactor(3);

        try {
            djc.getKeyspace();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getHost();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getRpcPort();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getUsername();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getPassword();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getColumnFamily();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getPageSize();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            djc.getPageSize();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }


        try {
            djc.getEntityClass();
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.host("localhost");

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.keyspace(KEYSPACE_NAME);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.columnFamily("test_page");

        try {
            djc.readConsistencyLevel("not valid CL");
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            djc.pageSize(0);
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            djc.pageSize(1 + Constants.DEFAULT_MAX_PAGE_SIZE);
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

            djc.pageSize(10);
        } catch (Exception e) {
            fail(e.getMessage());
        }


        djc.readConsistencyLevel(ConsistencyLevel.LOCAL_ONE.name());

        try {
            djc.writeConsistencyLevel("not valid CL");
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.writeConsistencyLevel(ConsistencyLevel.LOCAL_ONE.name());

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.columnFamily(COLUMN_FAMILY);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.bisectFactor(4);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.initialize();
    }

    @Test
    public void testWronglyMappedField() {

        DeepJobConfig<WronglyMappedTestEntity> djc = CassandraConfigFactory.create(WronglyMappedTestEntity.class).host
                (Constants.DEFAULT_CASSANDRA_HOST).rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT).keyspace(KEYSPACE_NAME).columnFamily(COLUMN_FAMILY);

        try {
            djc.initialize();

            fail();
        } catch (DeepNoSuchFieldException e) {
            // ok
            log.info("Correctly catched DeepNoSuchFieldException: " + e.getLocalizedMessage());
        }
    }

    @Test
    public void testValidationNotAnnotadedTestEntity() {
        DeepJobConfig<NotAnnotatedTestEntity> djc = CassandraConfigFactory.create(NotAnnotatedTestEntity.class)
                .keyspace("a").columnFamily("cf");
        try {
            djc.initialize();

            fail();
        } catch (AnnotationTypeMismatchException iae) {
            log.info("Correctly catched AnnotationTypeMismatchException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            log.error("Error", e);
            fail(e.getMessage());
        }
    }
}
