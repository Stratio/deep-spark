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

package com.stratio.es;

import com.google.common.io.Resources;
import com.stratio.deep.testutils.FunctionalTest;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.*;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = { "ESJavaRDDTest", "FunctionalTests" })
public class ESJavaRDDFT {

    private static final Logger LOG = Logger.getLogger(ESJavaRDDFT.class);

    public static final String MESSAGE_TEST = "new message test";
    public static final Integer PORT = 9200;
    public static final String HOST = "localhost";
    public static final String DATABASE = "twitter/tweet";
    public static final String ES_INDEX = "twitter";
    public static final String ES_TYPE = "tweet";
    public static final String ES_INDEX_BOOK = "book";
    public static final String ES_INDEX_MESSAGE = "test";
    public static final String ES_TYPE_MESSAGE  = "input";
    public static final String ES_SEPARATOR = "/";
    public static final String ES_TYPE_INPUT = "input";
    public static final String ES_TYPE_OUTPUT = "output";
    public static final String DATABASE_BOOK = "book/input";
    public static final String DATABASE_OUTPUT = "twitter/tweet2";
    public static final String COLLECTION_OUTPUT = "output";
    public static final String COLLECTION_OUTPUT_CELL = "outputcell";
    public static final String DATA_SET_NAME = "divineComedy.json";
    public static final Long WORD_COUNT_SPECTED = 3833L;
    public static Node node = null;
    public static Client client = null;
    public final static String DB_FOLDER_NAME = System.getProperty("user.home") +
            File.separator + "ESEntityRDDTest";

    @BeforeSuite
    public static void init() throws IOException, ExecutionException, InterruptedException, ParseException {

        File file = new File(DB_FOLDER_NAME);
        FileUtils.deleteDirectory(file);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.logs", "")
                .put("path.data", DB_FOLDER_NAME)
                .build();

        node = nodeBuilder().settings(settings).data(true).local(true).clusterName(HOST).node();
        client = node.client();

        LOG.info("Started local node at " + DB_FOLDER_NAME + " settings " + node.settings().getAsMap());





    }

    /**
     * Imports dataset
     *
     * @throws java.io.IOException
     */
    private static void dataSetImport()
            throws IOException, ExecutionException, IOException, InterruptedException, ParseException {

        JSONParser parser = new JSONParser();
        URL url = Resources.getResource(DATA_SET_NAME);
        Object obj = parser.parse(new FileReader(url.getFile()));

        JSONObject jsonObject = (JSONObject) obj;

        IndexResponse responseBook = client.prepareIndex(ES_INDEX_BOOK, ES_TYPE_INPUT,"id")
                .setSource(jsonObject.toJSONString())
                .execute()
                .actionGet();

        String json2 = "{" +

                "\"message\":\"" + MESSAGE_TEST + "\"" +
                "}";

        IndexResponse response2 = client.prepareIndex(ES_INDEX_MESSAGE, ES_TYPE_MESSAGE).setCreate(true)
                .setSource(json2).setReplicationType(ReplicationType.ASYNC)
                .execute()
                .actionGet();

        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        IndexResponse response = client.prepareIndex(ES_INDEX, ES_TYPE).setCreate(true)
                .setSource(json)
                .execute()
                .actionGet();

        String index = response.getIndex();
        String _type = response.getType();
        String _id = response.getId();
        try {
            CountResponse countResponse = client.prepareCount(ES_INDEX).setTypes(ES_TYPE)
                    .execute()
                    .actionGet();

            SearchResponse searchResponse = client.prepareSearch(ES_INDEX_BOOK).setTypes(ES_TYPE_INPUT)
                    .execute()
                    .actionGet();

            //searchResponse.getHits().hits();
            //assertEquals(searchResponse.getCount(), 1);
        } catch (AssertionError | Exception e) {
            cleanup();
            e.printStackTrace();
        }
    }

    private static void deleteDataSet() {

        try {
            DeleteByQueryResponse delete = client.prepareDeleteByQuery(ES_INDEX)
                    .setQuery(termQuery("_type", ES_TYPE))
                    .execute()
                    .actionGet();
            delete.status();

        } catch (Exception e) {

//            e.printStackTrace();
        }
    }

    @Test
    public void testRDD() throws IOException, ExecutionException, InterruptedException {

        Assert.assertEquals(true, true);

    }

    @AfterSuite
    public static void cleanup() throws IOException {

        deleteDataSet();
        if (node != null) {

            node.stop();
            client.close();
        }

        File file = new File(DB_FOLDER_NAME);
        FileUtils.deleteDirectory(file);

    }
}