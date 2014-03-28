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

import java.util.List;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.testentity.DomainEntity;
import com.stratio.deep.testentity.PageEntity;
import com.stratio.deep.rdd.CassandraJavaRDD;
import com.stratio.deep.rdd.CassandraRDD;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 13-feb-2014
 */
public final class WritingEntityToCassandra {
    private static Logger logger = Logger.getLogger(WritingEntityToCassandra.class);

    private WritingEntityToCassandra(){}

    public static void main(String[] args) {

        String job = "java:writingEntityToCassandra";

        String keyspaceName = "crawler";
        String inputTableName = "Page";
        String outputTableName = "listdomains";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties();
        DeepSparkContext deepContext = new DeepSparkContext(p.cluster, job, p.sparkHome, p.jarList);

        // --- INPUT RDD
        IDeepJobConfig inputConfig = DeepJobConfigFactory.create(PageEntity.class)
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(keyspaceName).table(inputTableName)
                .initialize();

        CassandraJavaRDD inputRDD = deepContext.cassandraJavaRDD(inputConfig);

        JavaPairRDD<String,PageEntity> pairRDD = inputRDD.map(new PairFunction<PageEntity,String,PageEntity>() {
            @Override
            public Tuple2<String,PageEntity> call(PageEntity e) {
                return new Tuple2<String, PageEntity>(e.getDomainName(),e);
            }
        });

        JavaPairRDD<String,Integer> numPerKey = pairRDD.groupByKey()
                .map(new PairFunction<Tuple2<String, List<PageEntity>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, List<PageEntity>> t) {
                        return new Tuple2<String, Integer>(t._1(), t._2().size());
                    }
                });

        // --- OUTPUT RDD
        IDeepJobConfig outputConfig = DeepJobConfigFactory.create(DomainEntity.class)
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(keyspaceName).table(outputTableName)
                .createTableOnWrite(false);

        if ( args.length > 0 ) {
            int batchSize = Integer.parseInt(args[0]);
            logger.info("EMAR WritingEntityToCassandra: using batch size: " + batchSize );
            outputConfig.batchSize( batchSize );
        }
        outputConfig.initialize();

        JavaRDD outputRDD = numPerKey.map(new Function<Tuple2<String, Integer>, DomainEntity>() {
            @Override
            public DomainEntity call(Tuple2<String, Integer> t) throws Exception {
                DomainEntity e = new DomainEntity();
                e.setDomain(t._1());
                e.setNumPages(t._2());
                return e;
            }
        });

        CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig);

        System.exit(0);
    }
}
