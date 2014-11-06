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
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.commons.utils.Pair;
import com.stratio.deep.mongodb.reader.MongoReader;

/**
 * Created by rcrespo on 29/10/14.
 */
public class MongoNativeCellExtractor implements IExtractor<Cells, ExtractorConfig<Cells>> {

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
    public int splitSize = 1;

    /**
     * The Max chunks.
     */
    public int maxChunks = 3;

    /**
     * The constant MONGODB_ID.
     */
    public static String MONGODB_ID = "_id";
    /**
     * The Reader.
     */
    private MongoReader reader;

    @Override
    public Partition[] getPartitions(ExtractorConfig<Cells> config) {
        MongoClient mongoClient = null;

        try {

            splitSize = config.getInteger(ExtractorConstants.SPLIT_SIZE) != null ? config.getInteger(ExtractorConstants
                    .SPLIT_SIZE) : splitSize;
            DBCollection collection;
            ServerAddress address = new ServerAddress(config.getString(ExtractorConstants.HOST));

            List<ServerAddress> addressList = new ArrayList<>();
            addressList.add(address);
            mongoClient = new MongoClient(addressList);

            mongoClient.setReadPreference(ReadPreference.nearest());
            DB db = mongoClient.getDB(config.getString(ExtractorConstants.CATALOG));
            collection = db.getCollection(config.getString(ExtractorConstants.TABLE));
            return isShardedCollection(collection) ? calculateShardChunks(collection,
                    config) : calculateSplits(collection, config);
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
     * @param config     the config
     * @return the deep partition [ ]
     */
    private DeepPartition[] calculateSplits(DBCollection collection, ExtractorConfig<Cells> config) {

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

        DeepPartition[] partitions = new DeepPartition[splitData.size() + 1];

        for (Object aSplitData : splitData) {

            BasicDBObject currentKey = (BasicDBObject) aSplitData;

            Object currentO = currentKey.get(MONGODB_ID);

            partitions[i] = new DeepPartition(config.getRddId(), i, new DeepTokenRange(lastKey, currentO, stringHosts));

            lastKey = currentO;
            i++;
        }
        QueryBuilder queryBuilder = QueryBuilder.start(MONGODB_ID);
        queryBuilder.greaterThanEquals(lastKey);
        partitions[i] = new DeepPartition(0, i, new DeepTokenRange(lastKey, null, stringHosts));
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
     * @param config     the config
     * @return the deep partition [ ]
     */
    private DeepPartition[] calculateShardChunks(DBCollection collection, ExtractorConfig<Cells> config) {

        DBCursor chuncks = getChunks(collection);

        Map<String, String[]> shards = getShards(collection);

        DeepPartition[] deepPartitions = new DeepPartition[chuncks.count()];
        int i = 0;
        boolean keyAssigned = false;
        String key = null;
        while (chuncks.hasNext()) {

            DBObject dbObject = chuncks.next();
            if (!keyAssigned) {
                Set<String> keySet = ((DBObject) dbObject.get("min")).keySet();
                for (String s : keySet) {
                    config.putValue("stratio_mongodb_key", s);
                    key = s;
                    keyAssigned = true;
                }
            }
            deepPartitions[i] = new DeepPartition(config.getRddId(), i, new DeepTokenRange(shards.get(dbObject.get
                    ("shard")),
                    ((DBObject) dbObject.get
                            ("min")).get(key),
                    ((DBObject) dbObject.get("max")).get(key)));
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
    public void initIterator(Partition dp, ExtractorConfig<Cells> config) {
        String database = config.getString(ExtractorConstants.DATABASE);
        String collection = config.getString(ExtractorConstants.COLLECTION);
        reader = new MongoReader(config.getString("stratio_mongodb_key"));
        reader.init(dp, database, collection);
    }

    @Override
    public void saveRDD(Cells cells) {

    }

    @Override
    public void initSave(ExtractorConfig<Cells> config, Cells first) {

    }

}
