package com.stratio.deep.config;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.testentity.MessageTestEntity;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Created by rcrespo on 18/06/14.
 */
@Test
public class GenericConfigFactoryMongoDBTest {

    private static final String DATATABASE_TEST = "test";

    private static final String HOST_TEST = "localhost:27017";

    private static final String HOST_TEST_2 = "localhost:27018";

    private static final String HOST_TEST_3 = "localhost:27019";

    private static final String COLLECTION_TEST = "collection";

    private static final String USER_TEST = "user";

    private static final String PASSWORD_TEST = "password";

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testDatabaseValidation() {
        IMongoDeepJobConfig<MessageTestEntity> djc = MongoConfigFactory.createMongoDB(MessageTestEntity.class);

        djc.host(HOST_TEST).collection(COLLECTION_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.database(DATATABASE_TEST);
        }

        djc.initialize();
    }

    @Test
    public void testCollectionValidation() {
        IMongoDeepJobConfig<MessageTestEntity> djc = MongoConfigFactory.createMongoDB(MessageTestEntity.class);


        djc.host(HOST_TEST).database(DATATABASE_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());
            djc.collection(COLLECTION_TEST);
        }

        djc.initialize();
    }

    @Test
    public void testHostValidation() {
        IMongoDeepJobConfig<MessageTestEntity> djc = MongoConfigFactory.createMongoDB(MessageTestEntity.class);


        djc.database(DATATABASE_TEST).collection(COLLECTION_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());


        }

        djc.host(HOST_TEST).host(HOST_TEST_2).host(HOST_TEST_3);

        djc.initialize();

        assertEquals(djc.getHost(), HOST_TEST);

        assertEquals(djc.getHostList().get(0), HOST_TEST);
        assertEquals(djc.getHostList().get(1), HOST_TEST_2);
        assertEquals(djc.getHostList().get(2), HOST_TEST_3);

        List<String> hostList = new ArrayList<>();

        hostList.add(HOST_TEST);
        hostList.add(HOST_TEST_2);
        hostList.add(HOST_TEST_3);

        IMongoDeepJobConfig<MessageTestEntity> djc2 = MongoConfigFactory.createMongoDB(MessageTestEntity.class);

        djc2.database(DATATABASE_TEST).collection(COLLECTION_TEST).host(hostList).initialize();

        assertEquals(djc2.getHostList().get(0), HOST_TEST);
        assertEquals(djc2.getHostList().get(1), HOST_TEST_2);
        assertEquals(djc2.getHostList().get(2), HOST_TEST_3);


    }

    @Test
    public void testEntity() {
        IMongoDeepJobConfig<MessageTestEntity> djc = MongoConfigFactory.createMongoDB(MessageTestEntity.class);

        djc.host(HOST_TEST).database(DATATABASE_TEST).collection(COLLECTION_TEST).username(USER_TEST).password(PASSWORD_TEST);


        djc.initialize();

        assertEquals(djc.getEntityClass(), MessageTestEntity.class);

        assertEquals(djc.getUsername(), USER_TEST);
        assertEquals(djc.getPassword(), PASSWORD_TEST);

        IMongoDeepJobConfig<Cells> djcCell = MongoConfigFactory.createMongoDB();

        djcCell.host(HOST_TEST).database(DATATABASE_TEST).collection(COLLECTION_TEST);

        djcCell.initialize();

        assertEquals(djcCell.getEntityClass(), Cells.class);


    }
}
