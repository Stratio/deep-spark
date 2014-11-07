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

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.impl.DeepPartition;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;
import com.stratio.deep.mongodb.partition.MongoPartition;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 30/10/14.
 *
 * @param <T> the type parameter
 */
public class MongoReader<T> {

    /**
     * The Mongo client.
     */
    private MongoClient mongoClient = null;
    /**
     * The Collection.
     */
    private DBCollection collection = null;
    /**
     * The Db.
     */
    private DB db = null;
    /**
     * The Db cursor.
     */
    private DBCursor dbCursor = null;


    private MongoDeepJobConfig mongoDeepJobConfig;

    /**
     * Instantiates a new Mongo reader.
     */
    public MongoReader(MongoDeepJobConfig mongoDeepJobConfig) {
        this.mongoDeepJobConfig = mongoDeepJobConfig;
    }

    /**
     * The type Connections.
     */
    public static class connections {

        //        private

    }


    /**
     * Close void.
     */
    public void close() {
        if (dbCursor != null) {
            dbCursor.close();
        }

        if (mongoClient != null) {
            mongoClient.close();
        }

    }

    /**
     * Has next.
     *
     * @return the boolean
     */
    public boolean hasNext() {
        return dbCursor.hasNext();
    }

    /**
     * Next cells.
     *
     * @return the cells
     */
    public DBObject next() {
       return dbCursor.next();
    }

    /**
     * Init void.
     *
     * @param partition      the partition
     */
    public void init(Partition partition) {
        try {

            List<ServerAddress> addressList = new ArrayList<>();

            for (String s : (List<String>) ((DeepPartition) partition).splitWrapper().getReplicas()) {
                addressList.add(new ServerAddress(s));
            }
            mongoClient = new MongoClient(addressList);
            mongoClient.setReadPreference(ReadPreference.nearest());
            db = mongoClient.getDB(mongoDeepJobConfig.getDatabase());
            collection = db.getCollection(mongoDeepJobConfig.getCollection());



            dbCursor = collection.find(generateFilterQuery((MongoPartition) partition),mongoDeepJobConfig.getInputFields());

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create query partition.
     *
     * @param partition the partition
     * @return the dB object
     */
    private DBObject createQueryPartition(MongoPartition partition) {

        QueryBuilder queryBuilderMin = QueryBuilder.start(partition.getKey());
        DBObject bsonObjectMin = queryBuilderMin.greaterThanEquals(partition.splitWrapper().getStartToken()).get();

        QueryBuilder queryBuilderMax = QueryBuilder.start(partition.getKey());
        DBObject bsonObjectMax = queryBuilderMax.lessThan(partition.splitWrapper().getEndToken()).get();

        QueryBuilder queryBuilder = QueryBuilder.start();
        if (partition.splitWrapper().getStartToken() != null) {
            queryBuilder.and(bsonObjectMin);
        }

        if (partition.splitWrapper().getEndToken() != null) {
            queryBuilder.and(bsonObjectMax);
        }

        return queryBuilder.get();
    }


    private DBObject generateFilterQuery(MongoPartition partition){

        if(mongoDeepJobConfig.getQuery() !=null){
            QueryBuilder queryBuilder = QueryBuilder.start();

            queryBuilder.and(createQueryPartition(partition),mongoDeepJobConfig.getQuery());

            return queryBuilder.get();
        }

        return createQueryPartition(partition);


    }

}


