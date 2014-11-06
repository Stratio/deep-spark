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
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
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

    public static final String DATA_SET_NAME = "aerospikeData.csv";


    @BeforeSuite
    public static void init() throws IOException {
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
    private static void dataSetImport() throws IOException {
        URL url = Resources.getResource(DATA_SET_NAME);
        Scanner scanner = new Scanner(new File(url.getFile()));
        Scanner lineScanner = null;
        int index = 0;
        while(scanner.hasNextLine()){
            lineScanner = new Scanner(scanner.nextLine());
            lineScanner.useDelimiter(",");
            Key key = null;
            Bin title = null;
            Bin author = null;
            Bin pages = null;
            while(lineScanner.hasNext()) {
                if(index == 0) {
                    key = new Key(NAMESPACE_BOOK, SET_NAME, lineScanner.nextInt());
                }
                if(index == 1) {
                    title = new Bin("title", lineScanner.next());
                }
                if(index == 2) {
                    author = new Bin("author", lineScanner.next());
                }
                if(index == 3) {
                    pages = new Bin("pages", lineScanner.nextInt());
                }
                index++;
            }
            index = 0;
            aerospike.put(null, key, title, author, pages);
        }
        Key key = new Key(NAMESPACE_TEST, SET_NAME, 1);
        Bin bin_number = new Bin("number", 1);
        Bin bin_text = new Bin("message", "new message test");
        aerospike.put(null, key, bin_number, bin_text);
        scanner.close();
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
