package com.stratio.deep.config;

import java.lang.annotation.AnnotationTypeMismatchException;

import com.stratio.deep.context.AbstractDeepSparkContextTest;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.entity.TestEntity;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.util.Constants;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

@Test(suiteName = "cassandraRddTests", groups = { "GenericDeepJobConfigTest" }, dependsOnGroups = { "CassandraJavaRDDTest" })
public class GenericDeepJobConfigTest extends AbstractDeepSparkContextTest {
    class NotAnnotatedTestEntity implements IDeepType {
	private static final long serialVersionUID = -2603126590709315326L;
    }

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testCorrectInitialisation() {
	IDeepJobConfig<TestEntity> djc = DeepJobConfigFactory.create(TestEntity.class).host(Constants.DEFAULT_CASSANDRA_HOST).rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
			.cqlPort(CassandraServer.CASSANDRA_CQL_PORT).keyspace("test_keyspace").columnFamily("test_page");

	djc.getConfiguration();
    }

    @Test
    public void testValidation() {
	IDeepJobConfig<TestEntity> djc = DeepJobConfigFactory.create(TestEntity.class);

	djc.host(null).rpcPort(null);

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
	    djc.getEntityClass();
	} catch (DeepIllegalAccessException e) {
	    log.info("Correctly catched DeepIllegalAccessException: " + e.getLocalizedMessage());
	} catch (Exception e) {
	    fail(e.getMessage());
	}

	try {
	    djc.getThriftFramedTransportSizeMB();
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

	djc.keyspace("test_keyspace");

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
	djc.initialize();
    }

    @Test
    public void testValidationNotAnnotadedTestEntity() {
	IDeepJobConfig<NotAnnotatedTestEntity> djc = DeepJobConfigFactory.create(NotAnnotatedTestEntity.class)
			.keyspace("a").columnFamily("cf");
	try {
	    djc.initialize();

	    fail();
	} catch (AnnotationTypeMismatchException iae) {
	    log.info("Correctly catched AnnotationTypeMismatchException: " + iae.getLocalizedMessage());
	} catch (Exception e) {
	    fail(e.getMessage());
	}
    }
}
