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

package com.stratio.deep.rdd.mongodb;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.List;

import com.google.common.io.Resources;

import com.mongodb.*;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.config.MongoConfigFactory;
import com.stratio.deep.context.MongoDeepSparkContext;
import com.stratio.deep.testentity.BookEntity;
import com.stratio.deep.testentity.MessageTestEntity;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.Network;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by rcrespo on 18/06/14.
 */
@Test
public class MongoEntityRDDTest implements Serializable {

    static MongodExecutable mongodExecutable = null;
    static MongoClient mongo = null;

    private DBCollection col = null;

    private static final String MESSAGE_TEST = "new message test";

    private static final Integer PORT = 27890;

    private static final String HOST = "localhost";

    private static final String DATABASE = "test";

    private static final String COLLECTION_INPUT = "input";

    private static final String COLLECTION_OUTPUT = "output";

    private static final String DATA_SET_NAME = "divineComedy.json";

    private final static String DB_FOLDER_NAME = System.getProperty("user.home") +
            File.separator + "mongoEntityRDDTest";

    private static final Long WORD_COUNT_SPECTED = 3833L;

    private static final String DATA_SET_URL = "http://docs.openstratio.org/resources/datasets/divineComedy.json";

    @BeforeClass
    public void init() throws IOException {
        Command command = Command.MongoD;

        MongodStarter starter = MongodStarter.getDefaultInstance();

        new File(DB_FOLDER_NAME).mkdirs();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .configServer(false)
                .replication(new Storage(DB_FOLDER_NAME, null, 0))
                .net(new Net(PORT, Network.localhostIsIPv6()))
                .cmdOptions(new MongoCmdOptionsBuilder()
                        .syncDelay(10)
                        .useNoPrealloc(true)
                        .useSmallFiles(true)
                        .useNoJournal(true)
                        .build())
                .build();


        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(command)
                .artifactStore(new ArtifactStoreBuilder()
                        .defaults(command)
                        .download(new DownloadConfigBuilder()
                                .defaultsForCommand(command)
                                .downloadPath("https://s3-eu-west-1.amazonaws.com/stratio-mongodb-distribution/")))
                .build();

        MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);

        mongodExecutable = null;


        mongodExecutable = runtime.prepare(mongodConfig);

        MongodProcess mongod = mongodExecutable.start();

//TODO : Uncomment when drone.io is ready
//        Files.forceDelete(new File(System.getProperty("user.home") +
//                File.separator + ".embedmongo"));


        mongo = new MongoClient(HOST, PORT);
        DB db = mongo.getDB(DATABASE);
        col = db.getCollection(COLLECTION_INPUT);
        col.save(new BasicDBObject("message", MESSAGE_TEST));


        dataSetImport();


    }

    /**
     * Imports dataset
     * @throws java.io.IOException
     */
    private static void dataSetImport() throws IOException {
        String dbName = "book";
        String collection = "input";
        URL url = Resources.getResource(DATA_SET_NAME);

        IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(PORT, Network.localhostIsIPv6()))
                .db(dbName)
                .collection(collection)
                .upsert(false)
                .dropCollection(true)
                .jsonArray(true)
                .importFile(url.getFile())
                .build();
        MongoImportExecutable mongoImportExecutable = MongoImportStarter.getDefaultInstance().prepare(mongoImportConfig);
        mongoImportExecutable.start();




    }

    @AfterClass
    public void cleanup() {

        if (mongodExecutable != null) {
            mongo.close();
            mongodExecutable.stop();
        }

        Files.forceDelete(new File(DB_FOLDER_NAME));
    }

    @Test
    public void testReadingRDD() {
        String hostConcat = HOST.concat(":").concat(PORT.toString());
	    MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<MessageTestEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(MessageTestEntity.class)
                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();

        JavaRDD<MessageTestEntity> inputRDDEntity = context.mongoJavaRDD(inputConfigEntity);

        assertEquals(col.count(), inputRDDEntity.cache().count());
        assertEquals(col.findOne().get("message"), inputRDDEntity.first().getMessage());

        context.stop();

    }

    @Test
    public void testWritingRDD() {


        String hostConcat = HOST.concat(":").concat(PORT.toString());

	    MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<MessageTestEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(MessageTestEntity.class)
                .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();

        RDD<MessageTestEntity> inputRDDEntity = context.mongoRDD(inputConfigEntity);

        IMongoDeepJobConfig<MessageTestEntity> outputConfigEntity = MongoConfigFactory.createMongoDB(MessageTestEntity.class)
                .host(hostConcat).database(DATABASE).collection(COLLECTION_OUTPUT).initialize();


        //Save RDD in MongoDB
        MongoEntityRDD.saveEntity(inputRDDEntity, outputConfigEntity);

        RDD<MessageTestEntity> outputRDDEntity = context.mongoRDD(outputConfigEntity);


        assertEquals(mongo.getDB(DATABASE).getCollection(COLLECTION_OUTPUT).findOne().get("message"),
                outputRDDEntity.first().getMessage());


        context.stop();


    }


    @Test
    public void testDataSet() {


        String hostConcat = HOST.concat(":").concat(PORT.toString());

	    MongoDeepSparkContext context = new MongoDeepSparkContext("local", "deepSparkContextTest");

        IMongoDeepJobConfig<BookEntity> inputConfigEntity = MongoConfigFactory.createMongoDB(BookEntity.class)
                .host(hostConcat).database("book").collection("input").initialize();

        RDD<BookEntity> inputRDDEntity = context.mongoRDD(inputConfigEntity);


//Import dataSet was OK and we could read it
        assertEquals(1, inputRDDEntity.count());


        List<BookEntity> books = inputRDDEntity.toJavaRDD().collect();


        BookEntity book = books.get(0);

        DBObject proObject = new BasicDBObject();
        proObject.put("_id",0);


//      tests subDocuments
        BSONObject result = mongo.getDB("book").getCollection("input").findOne(null, proObject);
        assertEquals(((DBObject) result.get("metadata")).get("author"), book.getMetadataEntity().getAuthor());


//      tests List<subDocuments>
        List<BSONObject> listCantos = (List<BSONObject>)result.get("cantos");
        BSONObject bsonObject = null;

        for(int i = 0; i< listCantos.size() ; i++){
            bsonObject = listCantos.get(i);
            assertEquals(bsonObject.get("canto"), book.getCantoEntities().get(i).getNumber());
            assertEquals(bsonObject.get("text"), book.getCantoEntities().get(i).getText());
        }



        context.stop();


    }

}
