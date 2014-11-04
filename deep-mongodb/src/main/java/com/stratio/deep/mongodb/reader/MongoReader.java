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

package com.stratio.deep.mongodb.reader;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Partition;
import org.bson.BSONObject;

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
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 30/10/14.
 */
public class MongoReader<T> {

    private MongoClient mongoClient = null;
    private DBCollection collection = null;
    private DB db = null;
    private String key = "_id";

    DBCursor dbCursor = null;


    public MongoReader () {

    }

    public MongoReader (String key) {
        if(key!=null){
            this.key=key;
        }

    }

    public void close (){
        if (dbCursor!=null){
            dbCursor.close();
        }
        if(mongoClient!=null){
            mongoClient.close();
        }

    }



    public boolean hasNext(){
        return dbCursor.hasNext();
    }

    public Cells next(){
        try {
            return UtilMongoDB.getCellFromBson(dbCursor.next(), collection.getFullName());
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void init(Partition partition, String host, String database, String collectionName) {
        try {



            ServerAddress address = new ServerAddress(host);
            List<ServerAddress> addressList = new ArrayList<>();

            for(String s: (List<String>)((DeepPartition)partition).splitWrapper().getReplicas()){
                addressList.add(new ServerAddress(s));
            }
            mongoClient = new MongoClient(addressList);
            mongoClient.setReadPreference(ReadPreference.nearest());
            db = mongoClient.getDB(database);
            collection = db.getCollection(collectionName);

            dbCursor = collection.find(createQueryPartition((DeepPartition)partition));

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

        private DBObject createQueryPartition(DeepPartition partition){

            QueryBuilder queryBuilderMin = QueryBuilder.start(key);
            DBObject bsonObjectMin = queryBuilderMin.greaterThanEquals(partition.splitWrapper().getStartToken()).get();

            QueryBuilder queryBuilderMax = QueryBuilder.start(key);
            DBObject bsonObjectMax = queryBuilderMax.lessThan(partition.splitWrapper().getEndToken()).get();

            QueryBuilder queryBuilder = QueryBuilder.start();
            if(partition.splitWrapper().getStartToken()!=null) {
                queryBuilder.and(bsonObjectMin);
            }

            if(partition.splitWrapper().getEndToken()!=null) {
                queryBuilder.and(bsonObjectMax);
            }


            return queryBuilder.get();
        }

    }


