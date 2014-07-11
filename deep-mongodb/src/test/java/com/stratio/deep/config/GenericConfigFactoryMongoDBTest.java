package com.stratio.deep.config;

import com.stratio.deep.testentity.MessageTestEntity;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

/**
 * Created by rcrespo on 18/06/14.
 */
public class GenericDeepJobConfigMongoDBTest {

    private static final String DATATABASE_TEST = "test";

    private static final String HOST_TEST = "localhost:27017";

    private static final String COLLECTION_TEST = "collection";

    private Logger log = Logger.getLogger(getClass());

    @Test
    public void testDatabaseValidation() {
        IMongoDeepJobConfig<MessageTestEntity> djc = DeepJobConfigFactory.createMongoDB(MessageTestEntity.class);

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
        IMongoDeepJobConfig<MessageTestEntity> djc = DeepJobConfigFactory.createMongoDB(MessageTestEntity.class);


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
        IMongoDeepJobConfig<MessageTestEntity> djc = DeepJobConfigFactory.createMongoDB(MessageTestEntity.class);


        djc.database(DATATABASE_TEST).collection(COLLECTION_TEST);

        try {
            djc.initialize();
            fail();
        } catch (IllegalArgumentException iae) {
            // OK
            log.info("Correctly catched IllegalArgumentException: " + iae.getLocalizedMessage());

            djc.host(HOST_TEST);
        }

        djc.initialize();


    }
}
