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

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.context.AbstractDeepSparkContextTest;
import com.stratio.deep.testentity.TestEntity;
import com.stratio.deep.testentity.WronglyMappedTestEntity;

import com.stratio.deep.core.embedded.CassandraServer;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepIllegalAccessException;
import com.stratio.deep.commons.exception.DeepNoSuchFieldException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

import com.stratio.deep.commons.utils.Constants;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.lang.annotation.AnnotationTypeMismatchException;
import java.util.HashMap;
import java.util.Map;

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
        values.put(ExtractorConstants.CQLPORT,  String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.RPCPORT,  String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));



        try {
            djc.setValues(values);
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            values = djc.getValues();
            values.put(ExtractorConstants.CREATE_ON_WRITE,String.valueOf(true));
            djc.setValues(values);
        } catch (Exception e) {
            fail(e.getMessage());
        }


    }

    @Test
    public void testInputColumnsExist() {
        ExtractorConfig<Cells> djc = new ExtractorConfig<>();

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, COLUMN_FAMILY);
        values.put(ExtractorConstants.CQLPORT,  String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.RPCPORT,  String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.INPUT_COLUMNS,"not_existent_col1");

        djc.setValues(values);

        try {

            fail();
        } catch (DeepNoSuchFieldException iae) {
            // OK
            log.info("Correctly catched DeepNoSuchFieldException: " + iae.getLocalizedMessage());
//            djc.inputColumns("domain_name", "response_time", "url");
        } catch (Exception e) {
            fail(e.getMessage());
        }


    }

    @Test
    public void testValidation() {

        ExtractorConfig<TestEntity> djc = new ExtractorConfig<>(TestEntity.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.PAGE_SIZE,String.valueOf(0));

        values.put(ExtractorConstants.HOST,  null);
        values.put(ExtractorConstants.RPCPORT,  null);
        values.put(ExtractorConstants.BISECT_FACTOR,String.valueOf(3));

        djc.setValues(values);

        try {
            djc.getValues().get(ExtractorConstants.KEYSPACE);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getValues().get(ExtractorConstants.HOST);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getValues().get(ExtractorConstants.RPCPORT);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getValues().get(ExtractorConstants.USERNAME);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getValues().get(ExtractorConstants.PASSWORD);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getValues().get(ExtractorConstants.COLUMN_FAMILY);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());

        }

        try {
            djc.getValues().get(ExtractorConstants.PAGE_SIZE);
        } catch (DeepIllegalAccessException e) {
            log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            djc.getValues().get(ExtractorConstants.PAGE_SIZE);
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

            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        djc.getValues().put(ExtractorConstants.HOST,"localhost");

        try {

            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
        djc.getValues().put(ExtractorConstants.RPCPORT,String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        djc.getValues().put(ExtractorConstants.CQLPORT,String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));


        try {

            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
        djc.getValues().put(ExtractorConstants.KEYSPACE,KEYSPACE_NAME);


        try {

            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }
        djc.getValues().put(ExtractorConstants.COLUMN_FAMILY,"test_page");


        try {
            //djc.readConsistencyLevel("not valid CL");

            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
           /* djc.pageSize(0);
            djc.initialize();*/
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
           /* djc.pageSize(1 + Constants.DEFAULT_MAX_PAGE_SIZE);
            djc.initialize();*/
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

//            djc.pageSize(10);
        } catch (Exception e) {
            fail(e.getMessage());
        }


        //djc.readConsistencyLevel(ConsistencyLevel.LOCAL_ONE.name());

        try {
//            djc.writeConsistencyLevel("not valid CL");
//            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            fail(e.getMessage());
        }

//        djc.writeConsistencyLevel(ConsistencyLevel.LOCAL_ONE.name());

        try {

            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
//            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.getValues().put(ExtractorConstants.BISECT_FACTOR,"4");
        } catch (Exception e) {
            fail(e.getMessage());
        }


    }

    @Test
    public void testWronglyMappedField() {

        ExtractorConfig<WronglyMappedTestEntity> djc = new ExtractorConfig<>(WronglyMappedTestEntity.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE,KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY,COLUMN_FAMILY);

        values.put(ExtractorConstants.HOST,  Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT,  String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.CQLPORT,  String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.BISECT_FACTOR,String.valueOf(3));

        djc.setValues(values);

        try {
            djc.setValues(values);

            fail();
        } catch (DeepNoSuchFieldException e) {
            // ok
            log.info("Correctly catched DeepNoSuchFieldException: " + e.getLocalizedMessage());
        }
    }

    @Test
    public void testValidationNotAnnotadedTestEntity() {
        ExtractorConfig<NotAnnotatedTestEntity> djc = new ExtractorConfig<>(NotAnnotatedTestEntity.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE,"a");
        values.put(ExtractorConstants.COLUMN_FAMILY,"cf");



        djc.setValues(values);


        try {

            fail();
        } catch (AnnotationTypeMismatchException iae) {
            log.info("Correctly catched AnnotationTypeMismatchException: " + iae.getLocalizedMessage());
        } catch (Exception e) {
            log.error("Error", e);
            fail(e.getMessage());
        }
    }
}
