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

package com.stratio.deep.mongodb.config;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.FILTER_QUERY;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.INPUT_KEY;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.READ_PREFERENCE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.REPLICA_SET;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.SORT;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.SPLIT_SIZE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USE_CHUNKS;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USE_SHARD;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USE_SPLITS;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.WRITE_MODE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.HOST;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.PORT;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.DATABASE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.COLLECTION;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;

public class MongoDeepJobConfigTest {


    private String host = "hostTest";
    private int port = 1234;
    private String database = "test";
    private String collection = "input";
    private String replicaSetName = "replica1";
    private String readPreference = ReadPreference.secondary().getName();
    private String sort = "{_id:1}";
    private String inputKey = "inputKey";
    private boolean useShards = true;
    private boolean useSplits = true;
    private boolean useChunks = true;
    private int splitSize = 20;
    private WriteConcern writeMode = WriteConcern.JOURNAL_SAFE;

    @Test
    public void testInitialize() throws Exception {

        MongoDeepJobConfig mongoDeepJobConfig = new MongoDeepJobConfig(Cells.class);

        mongoDeepJobConfig.initialize(getExtractorConfig());


        assertEquals(mongoDeepJobConfig.getHost(), host.concat(":").concat(String.valueOf(port)));
        assertEquals(mongoDeepJobConfig.getPort(), port);
        assertEquals(mongoDeepJobConfig.getDatabase(), database);
        assertEquals(mongoDeepJobConfig.getCollection(), collection);
        assertEquals(mongoDeepJobConfig.getReplicaSet(), replicaSetName);
        assertEquals(mongoDeepJobConfig.getReadPreference(), readPreference);
        assertEquals(mongoDeepJobConfig.getSort(), sort);
        assertEquals(mongoDeepJobConfig.getInputKey(), inputKey);
        assertEquals(mongoDeepJobConfig.isUseShards(), useShards);
        assertEquals(mongoDeepJobConfig.isCreateInputSplit(), useSplits);
        assertEquals(mongoDeepJobConfig.isSplitsUseChunks(), useChunks);
        assertEquals(mongoDeepJobConfig.getSplitSize().intValue(), splitSize);
        assertEquals(mongoDeepJobConfig.getWriteConcern(), writeMode);
    }





    private ExtractorConfig getExtractorConfig (){
        ExtractorConfig extractorConfig = new ExtractorConfig();
        extractorConfig.putValue(HOST, host);
        extractorConfig.putValue(PORT, port);
        extractorConfig.putValue(DATABASE, database);
        extractorConfig.putValue(COLLECTION, collection);


        extractorConfig.putValue(REPLICA_SET, replicaSetName);
        extractorConfig.putValue(READ_PREFERENCE, readPreference);
        extractorConfig.putValue(SORT, sort);
//        extractorConfig.putValue(FILTER_QUERY, ReadPreference.secondary().getName());
        extractorConfig.putValue(INPUT_KEY, inputKey);
        extractorConfig.putValue(USE_SHARD, useShards);
        extractorConfig.putValue(USE_SPLITS, useSplits);
        extractorConfig.putValue(USE_CHUNKS, useChunks);
        extractorConfig.putValue(SPLIT_SIZE, splitSize);
        extractorConfig.putValue(WRITE_MODE, writeMode);

        return extractorConfig;

    }



//    if (values.get(REPLICA_SET) != null) {
//        replicaSet(extractorConfig.getString(REPLICA_SET));
//    }
//
//    if (values.get(READ_PREFERENCE) != null) {
//        readPreference(extractorConfig.getString(READ_PREFERENCE));
//    }
//
//    if (values.get(SORT) != null) {
//        sort(extractorConfig.getString(SORT));
//    }
//
//    if (values.get(FILTER_QUERY) != null) {
//        filterQuery(extractorConfig.getFilterArray(FILTER_QUERY));
//    }
//
//    if (values.get(INPUT_KEY) != null) {
//        inputKey(extractorConfig.getString(INPUT_KEY));
//    }
//
//    if (values.get(IGNORE_ID_FIELD) != null && extractorConfig.getBoolean(IGNORE_ID_FIELD) == true) {
//        ignoreIdField();
//    }
//
//    if (values.get(INPUT_KEY) != null) {
//        inputKey(extractorConfig.getString(INPUT_KEY));
//    }
//
//    if (values.get(USE_SHARD) != null) {
//        useShards(extractorConfig.getBoolean(USE_SHARD));
//    }
//
//    if (values.get(USE_SPLITS) != null) {
//        createInputSplit(extractorConfig.getBoolean(USE_SPLITS));
//    }
//
//    if (values.get(USE_CHUNKS) != null) {
//        splitsUseChunks(extractorConfig.getBoolean(USE_CHUNKS));
//    }
//    if (values.get(SPLIT_SIZE) != null) {
//        pageSize(extractorConfig.getInteger(SPLIT_SIZE));
//    }
//
//    if (values.get(WRITE_MODE) != null) {
//        writeConcern((WriteConcern) extractorConfig.getValue(WriteConcern.class, WRITE_MODE));
//    }
}