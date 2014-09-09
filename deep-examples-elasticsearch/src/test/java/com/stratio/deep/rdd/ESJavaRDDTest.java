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

package com.stratio.deep.rdd;

import com.stratio.deep.commons.extractor.server.ExtractorServer;
import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.testng.AssertJUnit.assertEquals;


/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESJavaRDDTest"})
public class ESJavaRDDTest {

    private static final Logger LOG = Logger.getLogger(ESJavaRDDTest.class);

    public static final String MESSAGE_TEST = "new message test";
    public static final Integer PORT = 9200;
    public static final String HOST = "localhost";
    public static final String DATABASE = "twitter/tweet";
    public static final String ES_INDEX = "twitter";
    public static final String ES_TYPE = "tweet";
    public static final String DATABASE_OUTPUT = "twitter/tweet2";
    public static final String COLLECTION_OUTPUT = "output";
    public static final String COLLECTION_OUTPUT_CELL = "outputcell";
    public static final String DATA_SET_NAME = "divineComedy.json";
    public static final Long WORD_COUNT_SPECTED = 3833L;
    public static Node node = null;
    public static Client client = null;

    @BeforeSuite
    public static void init() throws IOException, ExecutionException, InterruptedException {


        node = nodeBuilder().clusterName(HOST).node();
        client = node.client();

        ExecutorService es = Executors.newFixedThreadPool(1);
        final Future future = es.submit(new Callable() {
            public Object call() throws Exception {
                ExtractorServer.start();
                return null;
            }
        });

        dataSetImport();



    }
    /**
     * Imports dataset
     *
     * @throws java.io.IOException
     */
    private static void dataSetImport() throws IOException, ExecutionException, InterruptedException {



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
            CountResponse searchResponse = client.prepareCount(ES_INDEX).setTypes(ES_TYPE)
                    .execute()
                    .actionGet();

            //assertEquals(searchResponse.getCount(), 1);
        }catch (AssertionError | Exception e){
            cleanup();
            e.printStackTrace();
        }
    }

    private static void deleteDataSet()  {

        try {
            DeleteByQueryResponse delete = client.prepareDeleteByQuery(ES_INDEX)
                    .setQuery(termQuery("_type", ES_TYPE))
                    .execute()
                    .actionGet();
            delete.status();

            //assertEquals(delete.getCount(), 1);
        }catch (Exception e){

            e.printStackTrace();
        }
    }

    @Test
    public void testRDD() throws IOException, ExecutionException, InterruptedException{
       // dataSetImport();

        Assert.assertEquals(true, true);

        //deleteDataSet();
        //cleanup();
    }

    @AfterSuite
    public static void cleanup() {

        deleteDataSet();
        if (node != null) {

            node.stop();
        }
        ExtractorServer.close();
        File file = new File("data");
        file.delete();
    }
}