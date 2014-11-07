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

package com.stratio.deep.mongodb.writer;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 5/11/14.
 */
public class MongoWriter {

    /**
     * The Mongo client.
     */
    private MongoClient mongoClient = null;
    /**
     * The Db collection.
     */
    private DBCollection dbCollection = null;

    /**
     * Instantiates a new Mongo writer.
     *
     * @param serverAddresses the server addresses
     * @param databaseName the database name
     * @param collectionName the collection name
     */
    public MongoWriter(List<ServerAddress> serverAddresses, String databaseName, String collectionName) {
        mongoClient = new MongoClient(serverAddresses);
        dbCollection = mongoClient.getDB(databaseName).getCollection(collectionName);
    }

    /**
     * Save void.
     *
     * @param dbObject the db object
     */
    public void save(DBObject dbObject) {
        dbCollection.insert(dbObject);
    }

    /**
     * Close void.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}
