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

package com.stratio.deep.examples.java;

import com.google.common.collect.Lists;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.cassandra.extractor.CassandraEntityExtractor;
import com.stratio.deep.testentity.DomainEntity;

import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 13-feb-2014
 */
public final class WritingEntityToCassandra {
    private static final Logger LOG = Logger.getLogger(WritingEntityToCassandra.class);
    public static List<Tuple2<String, Integer>> results;

    private WritingEntityToCassandra() {
    }

    /**
     * Application entry point.
     *
     * @param args the arguments passed to the application.
     */
    public static void main(String[] args) {
        doMain(args);
    }

    /**
     * This is the method called by both main and tests.
     *
     * @param args
     */
    public static void doMain(String[] args) {
        String job = "java:writingEntityToCassandra";

        String KEYSPACENAME = "crawler";
        String TABLENAME    = "listdomains";
        String CQLPORT      = "9042";
        String RPCPORT      = "9160";
        String HOST         = "127.0.0.1";

        final String outputTableName = "newlistdomains";




        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
	    DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        // --- INPUT RDD
        ExtractorConfig<DomainEntity> inputConfig = new ExtractorConfig(DomainEntity.class);

        inputConfig.setExtractorImplClass(CassandraEntityExtractor.class);
        //inputConfig.setEntityClass(TweetEntity.class);

        Map<String, Serializable> values = new HashMap<>();
        values.put(ExtractorConstants.KEYSPACE, KEYSPACENAME);
        values.put(ExtractorConstants.TABLE,    TABLENAME);
        values.put(ExtractorConstants.CQLPORT,  CQLPORT);
        values.put(ExtractorConstants.RPCPORT,  RPCPORT);
        values.put(ExtractorConstants.HOST,     HOST );

        inputConfig.setValues(values);

        RDD<DomainEntity> inputRDD = deepContext.createRDD(inputConfig);

        JavaPairRDD<String, DomainEntity> pairRDD = inputRDD.toJavaRDD().mapToPair(new PairFunction<DomainEntity, String,
                DomainEntity>() {
            @Override
            public Tuple2<String, DomainEntity> call(DomainEntity e) {
                return new Tuple2<String, DomainEntity>(e.getDomain(), e);
            }
        });

        JavaPairRDD<String, Integer> numPerKey = pairRDD.groupByKey()
                .mapToPair(new PairFunction<Tuple2<String, Iterable<DomainEntity>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Iterable<DomainEntity>> t) {
                        return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
                    }
                });

        results = numPerKey.collect();

        for (Tuple2<String, Integer> result : results) {
            LOG.info(result);
        }

        // --- OUTPUT RDD
        ExtractorConfig<DomainEntity> outputConfig = new ExtractorConfig(DomainEntity.class);

        JavaRDD<DomainEntity> outputRDD = numPerKey.map(new Function<Tuple2<String, Integer>, DomainEntity>() {
            @Override
            public DomainEntity call(Tuple2<String, Integer> t) throws Exception {
                DomainEntity e = new DomainEntity();
                e.setDomain(t._1());
                e.setNumPages(t._2());
                return e;
            }
        });

        deepContext.saveRDD(outputRDD.rdd(), outputConfig);


        deepContext.stop();
    }
}
