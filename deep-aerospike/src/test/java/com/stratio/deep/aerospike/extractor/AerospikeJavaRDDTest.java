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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.google.common.io.Resources;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.testng.Assert.assertEquals;

@Test(suiteName = "aerospikeRddTests", groups = { "AerospikeJavaRDDTest" })
public class AerospikeJavaRDDTest {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeJavaRDDTest.class);

    public static AerospikeClient aerospike = null;

    public static final Integer PORT = 3000;

    public static final String HOST = "127.0.0.1";

    public static final String NAMESPACE_TEST = "test";

    public static final String NAMESPACE_BOOK = "book";

    public static final String SET_NAME = "input";

    public static final String DATA_SET_NAME = "divineComedy.json";

    public static final Long WORD_COUNT_SPECTED = 3833L;


    @BeforeSuite
    public static void init() throws IOException, ParseException {
        aerospike = new AerospikeClient(HOST, PORT);
        deleteData();
        dataSetImport();
    }

    @Test
    public void testRDD() {
        assertEquals(true, true);
    }

    /**
     * Imports dataset
     *
     * @throws java.io.IOException
     */
    private static void dataSetImport() throws IOException, ParseException {
        URL url = Resources.getResource(DATA_SET_NAME);
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(url.getFile()));
        JSONObject jsonObject = (JSONObject) obj;

        String id = (String)jsonObject.get("id");
        JSONObject metadata = (JSONObject)jsonObject.get("metadata");
        JSONArray cantos = (JSONArray) jsonObject.get("cantos");

        Key key = new Key(NAMESPACE_BOOK, SET_NAME, id);
        Bin binId = new Bin("id", id);
        Bin binMetadata = new Bin("metadata", metadata);
        Bin binCantos = new Bin("cantos", cantos);
        aerospike.put(null, key, binId, binMetadata, binCantos);
        aerospike.createIndex(null, NAMESPACE_BOOK, SET_NAME, "id_idx", "id", IndexType.STRING);

        Key key2 = new Key(NAMESPACE_TEST, SET_NAME, 3);
        Bin bin_id = new Bin("_id", "3");
        Bin bin_number = new Bin("number", 3);
        Bin bin_text = new Bin("message", "new message test");
        aerospike.put(null, key2, bin_id, bin_number, bin_text);
        aerospike.createIndex(null, NAMESPACE_TEST, SET_NAME, "num_idx", "number", IndexType.NUMERIC);
        aerospike.createIndex(null, NAMESPACE_TEST, SET_NAME, "_id_idx", "_id", IndexType.STRING);
    }

    /**
     * Delete previously loaded data for starting with a fresh dataset.
     */
    private static void deleteData() {
        aerospike.scanAll(new ScanPolicy(), NAMESPACE_TEST, SET_NAME, new ScanCallback() {
            @Override
            public void scanCallback(Key key, Record record) throws AerospikeException {
                aerospike.delete(new WritePolicy(), key);
            }
        }, new String[] {});
        aerospike.scanAll(new ScanPolicy(), NAMESPACE_BOOK, SET_NAME, new ScanCallback() {
            @Override
            public void scanCallback(Key key, Record record) throws AerospikeException {
                aerospike.delete(new WritePolicy(), key);
            }
        }, new String[] {});
    }

    @AfterSuite
    public static void cleanup() {
        try {
            aerospike.close();
        } catch(Exception e) {
            LOG.error("Error while closing Aerospike client connection", e);
        }

    }
}
