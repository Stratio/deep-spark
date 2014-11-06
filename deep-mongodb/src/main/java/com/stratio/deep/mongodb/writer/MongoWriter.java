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
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 5/11/14.
 */
public class MongoWriter {

    private MongoClient mongoClient = null;
    private DBCollection dbCollection = null;
    private DB db = null;
    private String key = "_id";

    /**
     * Save void.
     *
     * @param cells the cells
     * @throws IllegalAccessException    the illegal access exception
     * @throws InvocationTargetException the invocation target exception
     * @throws InstantiationException    the instantiation exception
     */
    public void save(Cells cells) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        dbCollection.insert((DBObject) UtilMongoDB.getBsonFromCell(cells));

    }

    /**
     * Close void.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    /**
     * Init save.
     *
     * @param host       the host
     * @param port       the port
     * @param database   the database
     * @param collection the collection
     * @throws UnknownHostException the unknown host exception
     */
    public void initSave(List<String> host, int port, String database, String collection) throws UnknownHostException {
        mongoClient = new MongoClient(host.get(0));
        dbCollection = mongoClient.getDB(database).getCollection(collection);

    }
}
