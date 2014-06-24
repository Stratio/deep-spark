package com.stratio.deep.rdd.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.testentity.MesageTestEntity;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * Created by rcrespo on 18/06/14.
 */
@Test
public class MongoEntityRDDTest {

    static MongodExecutable mongodExecutable = null;
    static MongoClient mongo = null;

    private DBCollection col = null;

//    static  context = null;

    private static final String MESSAGE_TEST = "new message test";

    private static final Integer port = 27890;

    private static final String host = "localhost";

    private static final String database = "test";

    private static final String collection = "input";

    private static final String collectionOutput = "output";

    private final static String DB_FOLDER_NAME = System.getProperty("user.home") +
            File.separator + "mongoEntityRDDTest";

    @BeforeClass
    public void init() throws IOException {
        MongodStarter starter = MongodStarter.getDefaultInstance();

        new File(DB_FOLDER_NAME).mkdirs();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .replication(new Storage(DB_FOLDER_NAME, null, 0))
                .net(new Net(port, Network.localhostIsIPv6()))
                .build();

        mongodExecutable = starter.prepare(mongodConfig);

        Files.forceDelete(new File(System.getProperty("user.home") +
                File.separator + ".embedmongo"));

        MongodProcess mongod = mongodExecutable.start();

        mongo = new MongoClient(host, port);
        DB db = mongo.getDB(database);
        col = db.createCollection(collection, new BasicDBObject());
        col.save(new BasicDBObject("message", MESSAGE_TEST));
    }

    @AfterClass
    public void cleanup() {
        Files.forceDelete(new File(DB_FOLDER_NAME));
    }

    @Test
    public void testReadingRDD() {
        String hostConcat = host.concat(":").concat(port.toString());
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<MesageTestEntity> inputConfigEntity = DeepJobConfigFactory.createMongoDB(MesageTestEntity.class)
                .host(hostConcat).database(database).collection(collection).initialize();

        MongoJavaRDD<MesageTestEntity> inputRDDEntity = context.mongoJavaRDD(inputConfigEntity);

        assertEquals(col.count(), inputRDDEntity.cache().count());
        assertEquals(col.findOne().get("message"), inputRDDEntity.first().getMessage());

        context.stop();

    }

    @Test
    public void testWritingRDD() {


        String hostConcat = host.concat(":").concat(port.toString());

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<MesageTestEntity> inputConfigEntity = DeepJobConfigFactory.createMongoDB(MesageTestEntity.class)
                .host(hostConcat).database(database).collection(collection).initialize();

        MongoJavaRDD<MesageTestEntity> inputRDDEntity = context.mongoJavaRDD(inputConfigEntity);

        IMongoDeepJobConfig<MesageTestEntity> outputConfigEntity = DeepJobConfigFactory.createMongoDB(MesageTestEntity.class)
                .host(hostConcat).database(database).collection(collectionOutput).initialize();


        //Save RDD in MongoDB
        MongoEntityRDD.saveEntity(inputRDDEntity, outputConfigEntity);

        MongoJavaRDD<MesageTestEntity> outputRDDEntity = context.mongoJavaRDD(outputConfigEntity);


        assertEquals(mongo.getDB(database).getCollection(collectionOutput).findOne().get("message"),
                outputRDDEntity.first().getMessage());


        context.stop();


    }

    @AfterClass
    public void end() {
        if (mongodExecutable != null) {
            mongodExecutable.stop();
        }

    }


}
