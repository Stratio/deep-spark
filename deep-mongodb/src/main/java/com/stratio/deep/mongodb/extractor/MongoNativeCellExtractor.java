/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.mongodb.extractor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.Partition;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.commons.utils.Pair;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.partition.MongoPartition;
import com.stratio.deep.mongodb.reader.MongoReader;
import com.stratio.deep.mongodb.writer.MongoWriter;

/**
 * Created by rcrespo on 29/10/14.
 */
public class MongoNativeCellExtractor<S extends BaseConfig<Cells>> implements IExtractor<Cells,S> {

    /**
     * The constant SHARDED.
     */
    public static String SHARDED = "sharded";
    /**
     * The constant NAME_SPACE.
     */
    public static String NAME_SPACE = "ns";

    /**
     * The constant SPLIT_KEYS.
     */
    public static final String SPLIT_KEYS = "splitKeys";

    /**
     * The Split size.
     */
    public int splitSize = 10;

    /**
     * The constant MONGODB_ID.
     */
    public static String MONGODB_ID = "_id";
    /**
     * The Reader.
     */
    private MongoReader reader;

    private MongoWriter writer;

    private MongoDeepJobConfig<Cells> mongoDeepJobConfig = new MongoDeepJobConfig<>(Cells.class);

    @Override
    public Partition[] getPartitions(S config) {
        MongoClient mongoClient = null;

        try {

            initMongoDeepJobConfig(config);

            splitSize = mongoDeepJobConfig.getPageSize()>0 ? mongoDeepJobConfig.getPageSize() : splitSize;

            DBCollection collection;
            ServerAddress address = new ServerAddress(mongoDeepJobConfig.getHost());

            List<ServerAddress> addressList = new ArrayList<>();
            addressList.add(address);
            mongoClient = new MongoClient(addressList);

            mongoClient.setReadPreference(ReadPreference.nearest());
            DB db = mongoClient.getDB(mongoDeepJobConfig.getDatabase());
            collection = db.getCollection(mongoDeepJobConfig.getCollection());
            return isShardedCollection(collection) ? calculateShardChunks(collection) : calculateSplits(collection);
        } catch (UnknownHostException e) {

            e.printStackTrace();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }

        }
        return null;

    }

    /**
     * Is sharded collection.
     *
     * @param collection the collection
     * @return the boolean
     */
    private boolean isShardedCollection(DBCollection collection) {

        DB config = collection.getDB().getMongo().getDB("config");
        DBCollection configCollections = config.getCollection("collections");

        DBObject dbObject = configCollections.findOne(new BasicDBObject(MONGODB_ID, collection.getFullName()));
        return dbObject != null;
    }

    /**
     * Gets shards.
     *
     * @param collection the collection
     * @return the shards
     */
    private Map<String, String[]> getShards(DBCollection collection) {
        DB config = collection.getDB().getSisterDB("config");
        DBCollection configShards = config.getCollection("shards");

        DBCursor cursorShards = configShards.find();

        Map<String, String[]> map = new HashMap<>();
        while (cursorShards.hasNext()) {
            DBObject currentShard = cursorShards.next();
            String currentHost = (String) currentShard.get("host");
            int slashIndex = currentHost.indexOf("/");
            if (slashIndex > 0) {
                map.put((String) currentShard.get(MONGODB_ID),
                        currentHost.substring(slashIndex + 1).split(","));
            }
        }
        return map;
    }

    /**
     * Gets chunks.
     *
     * @param collection the collection
     * @return the chunks
     */
    private DBCursor getChunks(DBCollection collection) {
        DB config = collection.getDB().getSisterDB("config");
        DBCollection configChunks = config.getCollection("chunks");
        return configChunks.find(new BasicDBObject("ns", collection.getFullName()));
    }

    /**
     * Calculate splits.
     *
     * @param collection the collection
     * @return the deep partition [ ]
     */
    private DeepPartition[] calculateSplits(DBCollection collection) {

        BasicDBList splitData = getSplitData(collection);
        List<ServerAddress> serverAddressList = collection.getDB().getMongo().getServerAddressList();

        //Estamos intentado hacer splitVector desde mongos
        //a una coleccion no fragmentada
        if (splitData == null) {
            Pair<BasicDBList, List<ServerAddress>> pair = getSplitDataCollectionShardEnviroment(getShards(collection),
                    collection.getDB().getName(),
                    collection.getName());
            splitData = pair.left;
            serverAddressList = pair.right;
        }

        Object lastKey = null; // Lower boundary of the first min split

        List<String> stringHosts = new ArrayList<>();

        for (ServerAddress serverAddress : serverAddressList) {
            stringHosts.add(serverAddress.toString());
        }
        int i = 0;

        MongoPartition[] partitions = new MongoPartition[splitData.size() + 1];

        for (Object aSplitData : splitData) {

            BasicDBObject currentKey = (BasicDBObject) aSplitData;

            Object currentO = currentKey.get(MONGODB_ID);

            partitions[i] = new MongoPartition(mongoDeepJobConfig.getRddId(), i, new DeepTokenRange(lastKey,
                    currentO, stringHosts), MONGODB_ID);

            lastKey = currentO;
            i++;
        }
        QueryBuilder queryBuilder = QueryBuilder.start(MONGODB_ID);
        queryBuilder.greaterThanEquals(lastKey);
        partitions[i] = new MongoPartition(0, i, new DeepTokenRange(lastKey, null, stringHosts), MONGODB_ID);
        return partitions;
    }

    /**
     * Gets split data.
     *
     * @param collection the collection
     * @return the split data
     */
    private BasicDBList getSplitData(DBCollection collection) {

        final DBObject cmd = BasicDBObjectBuilder.start("splitVector", collection.getFullName())
                .add("keyPattern", new BasicDBObject(MONGODB_ID, 1))
                .add("force", false)
                .add("maxChunkSize", splitSize)
                .get();

        CommandResult splitVectorResult = collection.getDB().getSisterDB("admin").command(cmd);
        return (BasicDBList) splitVectorResult.get(SPLIT_KEYS);

    }

    /**
     * Gets split data collection shard enviroment.
     *
     * @param shards         the shards
     * @param dbName         the db name
     * @param collectionName the collection name
     * @return the split data collection shard enviroment
     */
    private Pair<BasicDBList, List<ServerAddress>> getSplitDataCollectionShardEnviroment(Map<String, String[]> shards,
                                                                                         String dbName,
                                                                                         String collectionName) {
        MongoClient mongoClient = null;
        try {
            Set<String> keys = shards.keySet();

            for (String key : keys) {

                List<ServerAddress> addressList = getServerAddressList(Arrays.asList(shards.get(key)));

                mongoClient = new MongoClient(addressList);

                BasicDBList dbList = getSplitData(mongoClient.getDB(dbName).getCollection(collectionName));

                if (dbList != null) {
                    return Pair.create(dbList, addressList);
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }

        }

        return null;

    }

    /**
     * Calculates shard chunks.
     *
     * @param collection the collection
     * @return the deep partition [ ]
     */
    private DeepPartition[] calculateShardChunks(DBCollection collection) {

        DBCursor chuncks = getChunks(collection);

        Map<String, String[]> shards = getShards(collection);

        MongoPartition[] deepPartitions = new MongoPartition[chuncks.count()];
        int i = 0;
        boolean keyAssigned = false;
        String key = null;
        while (chuncks.hasNext()) {

            DBObject dbObject = chuncks.next();
            if (!keyAssigned) {
                Set<String> keySet = ((DBObject) dbObject.get("min")).keySet();
                for (String s : keySet) {
                    key = s;
                    keyAssigned = true;
                }
            }
            deepPartitions[i] = new MongoPartition(mongoDeepJobConfig.getRddId(), i, new DeepTokenRange(shards.get(dbObject.get
                    ("shard")),
                    ((DBObject) dbObject.get
                            ("min")).get(key),
                    ((DBObject) dbObject.get("max")).get(key)), key);
            i++;
        }
        return deepPartitions;
    }

    /**
     * Gets server address list.
     *
     * @param addressStringList the address string list
     * @return the server address list
     * @throws UnknownHostException the unknown host exception
     */
    private List<ServerAddress> getServerAddressList(List<String> addressStringList) throws UnknownHostException {

        List<ServerAddress> addressList = new ArrayList<>();

        for (String addressString : addressStringList) {
            addressList.add(new ServerAddress(addressString));
        }
        return addressList;
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Cells next() {
        return reader.next();
    }

    @Override
    public void close() {
        reader.close();
    }

    @Override
    public void initIterator(Partition dp, S config) {

        initMongoDeepJobConfig(config);

        reader = new MongoReader();
        reader.init(dp, mongoDeepJobConfig.getDatabase(), mongoDeepJobConfig.getCollection());
    }

    private void initMongoDeepJobConfig(S config){

        if(config instanceof ExtractorConfig){
            mongoDeepJobConfig.initialize((ExtractorConfig) config);
        }else{
            mongoDeepJobConfig = (MongoDeepJobConfig<Cells>) config;
        }
    }

    @Override
    public void saveRDD(Cells cells) {
        writer.save(cells);
    }

    @Override
    public void initSave(S config, Cells first) {
        writer = new MongoWriter(null, null, null);
    }

}
