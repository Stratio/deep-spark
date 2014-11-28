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

package com.stratio.deep.examples.java.extractorconfig.mongodb;

import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.mongodb.extractor.MongoEntityExtractor;
import com.stratio.deep.utils.ContextProperties;

/**
 * Example class to read an entity from a mongoDB replica set
 */
public final class ReadingEntityFromMongoDBReplicaSet {
    private static final Logger LOG = Logger.getLogger(ReadingEntityFromMongoDBReplicaSet.class);

    private ReadingEntityFromMongoDBReplicaSet() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {

        // Spark jobName
        String job = "java:readingEntityFromMongoDBReplicaSet";

        // Connect to a replica set and provide three seed nodes
        String host1 = "localhost:47017";
        String host2 = "localhost:47018";
        String host3 = "localhost:47019";

        // database
        String database = "test";

        // collection
        String inputCollection = "input";

        // replica set name
        String replicaSet = "s1";

        // Recommended read preference. If the primary node go down, can still read from secundaries
        String readPreference = "primaryPreferred";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);

        // creates Deep Spark Context (spark context wrapper)
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        // can give a list of host.
        //TODO Review
        //        DeepJobConfig inputConfigEntity = MongoConfigFactory.createMongoDB(MessageEntity.class).host(host1).host(host2).host(host3)
        //                .database(database).collection(inputCollection).replicaSet(replicaSet).readPreference(readPreference).initialize();

        ExtractorConfig<MessageTestEntity> inputConfigEntity = new ExtractorConfig<>(MessageTestEntity.class);
        inputConfigEntity.putValue(ExtractorConstants.HOST, new String[] { host1, host2, host3 })
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, inputCollection)
                .putValue(ExtractorConstants.REPLICA_SET, replicaSet)
                .putValue(ExtractorConstants.READ_PREFERENCE, readPreference)
                .setExtractorImplClass(MongoEntityExtractor.class);

        // createJavaRDD
        RDD<MessageTestEntity> inputRDDEntity = deepContext.createRDD(inputConfigEntity);

        LOG.info("count : " + inputRDDEntity.cache().count());

        deepContext.stop();

    }
}
