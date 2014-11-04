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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.spark.Partition;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.splitter.MongoCollectionSplitter;
import com.mongodb.hadoop.splitter.SplitFailedException;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.reader.MongoReader;

/**
 * Created by rcrespo on 29/10/14.
 */
public class MongoNativeCellExtractor implements IExtractor<Cells, ExtractorConfig<Cells>> {


    public static String SHARDED = "sharded";
    public static String NAME_SPACE = "ns";

    public static final String SPLIT_KEYS = "splitKeys";

    public int splitSize = 10;

    public static String MONGODB_ID = "_id";
    private MongoReader reader;



    @Override
    public Partition[] getPartitions(ExtractorConfig<Cells> config) {

        try {
            MongoClient mongo;

            DBCollection collection;
            ServerAddress address = new ServerAddress(config.getString(ExtractorConstants.HOST));

            List<ServerAddress> addressList = new ArrayList<>();
            addressList.add(address);
            mongo = new MongoClient(addressList);
            mongo.setReadPreference(ReadPreference.nearest());
            DB db = mongo.getDB(config.getString(ExtractorConstants.CATALOG));
            collection = db.getCollection(config.getString(ExtractorConstants.TABLE));
            boolean sharded = checkIsCollectionSharded(collection);
            return sharded?calculateShardSplits(collection, config):calculateSplits(collection, config);
        } catch (UnknownHostException e) {

            e.printStackTrace();
        }
        return null;

    }

    private boolean checkIsCollectionSharded(DBCollection collection){

        DB config = collection.getDB().getMongo().getDB("config");
        DBCollection configCollections = config.getCollection("collections");

        DBObject dbObject = configCollections.findOne(new BasicDBObject(MONGODB_ID, collection.getFullName()));
        System.out.println(dbObject);
        return dbObject!=null;
    }


    private Map<String, String[]> getShards (DBCollection collection){
        DB config = collection.getDB().getSisterDB("config");
        DBCollection configShards = config.getCollection("shards");

        DBCursor cursorShards = configShards.find();

        Map<String, String[]> map = new HashMap<>();
        while(cursorShards.hasNext()){
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

    private DBCursor getChunks (DBCollection collection){
        DB config = collection.getDB().getSisterDB("config");
        DBCollection configChunks = config.getCollection("chunks");
        return configChunks.find(new BasicDBObject("ns", collection.getFullName()));
    }

    private DeepPartition[] calculateSplits(DBCollection collection, ExtractorConfig<Cells> config){

        final DBObject cmd = BasicDBObjectBuilder.start("splitVector", collection.getFullName())
                .add("keyPattern", new BasicDBObject(MONGODB_ID,1))
                .add("force", false)
                .add("maxChunkSize", splitSize)
                .get();
        CommandResult splitVectorResult = collection.getDB().getSisterDB("admin").command(cmd);

        BasicDBList splitData = (BasicDBList) splitVectorResult.get(SPLIT_KEYS);

        Object lastKey = null; // Lower boundary of the first min split
        List<ServerAddress> serverAddressList = collection.getDB().getMongo().getServerAddressList();

        List<String> stringHosts = new ArrayList<>();

        for(ServerAddress serverAddress : serverAddressList){
            stringHosts.add(serverAddress.toString());
        }
        int i = 0;

        DeepPartition[] partitions = new DeepPartition[splitData.size()+1];

        for (Object aSplitData : splitData) {

            System.out.println(aSplitData);
            BasicDBObject currentKey = (BasicDBObject) aSplitData;


            Object currentO = currentKey.get(MONGODB_ID);


            partitions[i] = new DeepPartition(config.getRddId(),i, new DeepTokenRange(lastKey, currentO, stringHosts));

            lastKey = currentO;
            i++;
        }
        QueryBuilder queryBuilder = QueryBuilder.start(MONGODB_ID);
        queryBuilder.greaterThanEquals(lastKey);
        partitions[i] = new DeepPartition(0,i, new DeepTokenRange(lastKey, null, stringHosts));

        return partitions;
    }

    private DeepPartition[] calculateShardSplits(DBCollection collection, ExtractorConfig<Cells> config){

        DBCursor chuncks = getChunks(collection);

        Map<String, String[]> shards = getShards(collection);

        DeepPartition[] deepPartitions = new DeepPartition[chuncks.count()];
        int i = 0;
        boolean keyAssigned = false;
        String key = null;
        while(chuncks.hasNext()){

            DBObject dbObject = chuncks.next();
            if(!keyAssigned){
                Set<String> keySet = ((DBObject)dbObject.get("min")). keySet();
                for(String s : keySet){
                    config.putValue("stratio_mongodb_key", s);
                    key = s;
                    keyAssigned = true;
                }
            }

            deepPartitions[i] = new DeepPartition (config.getRddId(),i, new DeepTokenRange(shards.get(dbObject.get
                            ("shard")),
                    ((DBObject)dbObject.get
                    ("min")).get(key),
                    ((DBObject)dbObject.get("max")).get(key)));
            i++;
        }
        return deepPartitions;
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
                    String host = config.getString(ExtractorConstants.HOST);
                    String database = config.getString(ExtractorConstants.DATABASE);
                    String collection = config.getString(ExtractorConstants.COLLECTION);
        reader = new MongoReader(config.getString("stratio_mongodb_key"));
        reader.init(dp, host, database, collection);
    }


    @Override
    public void saveRDD(Cells cells) {

    }

    @Override
    public void initSave(ExtractorConfig<Cells> config, Cells first) {

    }

}
