package com.stratio.deep.aerospike.config;

import com.stratio.deep.aerospike.testentity.MessageTestEntity;
import com.stratio.deep.commons.entity.Cells;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test
public class GenericConfigFactoryAerospikeTest {

    private static final String HOST_TEST = "localhost";

    private static final int PORT_TEST = 3000;

    private static final String HOST_TEST_2 = "localhost";

    private static final int PORT_TEST_2 = 3000;

    private static final String HOST_TEST_3 = "localhost";

    private static final int PORT_TEST_3 = 3000;

    private static final String NAMESPACE_TEST = "test";

    private static final String SET_TEST = "books";

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testNamespaceValidation() {
        AerospikeDeepJobConfig<MessageTestEntity> djc = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc.host(HOST_TEST).port(PORT_TEST).set(SET_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.namespace(NAMESPACE_TEST);
        }

        djc.initialize();
    }

    @Test
    public void testSetValidation() {
        AerospikeDeepJobConfig<MessageTestEntity> djc = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc.host(HOST_TEST).port(PORT_TEST).namespace(NAMESPACE_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.set(SET_TEST);
        }

        djc.initialize();
    }

    @Test
    public void testHostValidation() {
        AerospikeDeepJobConfig<MessageTestEntity> djc = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc.namespace(NAMESPACE_TEST).set(SET_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

        }

        djc.host(HOST_TEST).port(PORT_TEST).host(HOST_TEST_2).port(PORT_TEST_2).host(HOST_TEST_3).port(PORT_TEST_3);

        djc.initialize();

        assertEquals(djc.getHost(), HOST_TEST);

        assertEquals(djc.getHostList().get(0), HOST_TEST);
        assertEquals(djc.getHostList().get(1), HOST_TEST_2);
        assertEquals(djc.getHostList().get(2), HOST_TEST_3);

        List<String> hostList = new ArrayList<>();

        hostList.add(HOST_TEST);
        hostList.add(HOST_TEST_2);
        hostList.add(HOST_TEST_3);

        AerospikeDeepJobConfig<MessageTestEntity> djc2 = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc2.namespace(NAMESPACE_TEST).set(SET_TEST).host(hostList).initialize();

        assertEquals(djc2.getHostList().get(0), HOST_TEST);
        assertEquals(djc2.getHostList().get(1), HOST_TEST_2);
        assertEquals(djc2.getHostList().get(2), HOST_TEST_3);

    }

    @Test
    public void testPortValidation() {
        AerospikeDeepJobConfig<MessageTestEntity> djc = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc.namespace(NAMESPACE_TEST).host(HOST_TEST).set(SET_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

        }

        djc.port(PORT_TEST);

        djc.initialize();

    }

    @Test
    public void testHostPortCardinalityValidation() {
        AerospikeDeepJobConfig<MessageTestEntity> djc = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc.namespace(NAMESPACE_TEST).host(HOST_TEST).port(PORT_TEST).host(HOST_TEST_2).set(SET_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

        }

        djc.port(PORT_TEST_2);

        djc.initialize();

    }

    @Test
    public void testEntity() {
        AerospikeDeepJobConfig<MessageTestEntity> djc = AerospikeConfigFactory.createAerospike(MessageTestEntity.class);

        djc.host(HOST_TEST).port(PORT_TEST).namespace(NAMESPACE_TEST).set(SET_TEST);

        djc.initialize();

        assertEquals(djc.getEntityClass(), MessageTestEntity.class);

        AerospikeDeepJobConfig<Cells> djcCell = AerospikeConfigFactory.createAerospike();

        djcCell.host(HOST_TEST).port(PORT_TEST).namespace(NAMESPACE_TEST).set(SET_TEST);

        djcCell.initialize();

        assertEquals(djcCell.getEntityClass(), Cells.class);
    }
}
