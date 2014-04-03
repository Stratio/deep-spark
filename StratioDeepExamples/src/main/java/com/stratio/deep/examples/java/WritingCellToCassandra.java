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
import com.stratio.deep.testentity.Cell;
import com.stratio.deep.testentity.Cells;
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
 * Date..: 3-mar-2014
 */
public final class WritingCellToCassandra {
    private static Logger logger = Logger.getLogger(WritingCellToCassandra.class);

    private WritingCellToCassandra(){}

    public static void main(String[] args) {
        String job = "java:writingCellToCassandra";

        String keyspaceName = "crawler";
        String inputTableName = "Page";
        String outputTableName = "newlistdomains";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties();
        DeepSparkContext deepContext = new DeepSparkContext(p.cluster, job, p.sparkHome, p.jarList);

        // --- INPUT RDD
        IDeepJobConfig inputConfig = DeepJobConfigFactory.create()
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(keyspaceName).table(inputTableName)
                .initialize();

        CassandraJavaRDD inputRDD = deepContext.cassandraJavaRDD(inputConfig);

        JavaPairRDD<String,Cells> pairRDD = inputRDD.map(new PairFunction<Cells,String,Cells>() {
            @Override
            public Tuple2<String,Cells> call(Cells c) {
                return new Tuple2<String, Cells>((String) c.getCellByName("domainName")
                        .getCellValue(),c);
            }
        });

        JavaPairRDD<String,Integer> numPerKey = pairRDD.groupByKey()
                .map(new PairFunction<Tuple2<String, List<Cells>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, List<Cells>> t) {
                        return new Tuple2<String, Integer>(t._1(), t._2().size());
                    }
                });

        // --- OUTPUT RDD
        IDeepJobConfig outputConfig = DeepJobConfigFactory.create()
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(keyspaceName).table(outputTableName)
                .createTableOnWrite(true);

        if ( args.length > 0 ) {
                int batchSize = Integer.parseInt(args[0]);
                logger.info("EMAR WritingCellToCassandra: using batch size: " + batchSize );
                outputConfig.batchSize( batchSize );
        }
        outputConfig.initialize();

        JavaRDD outputRDD = numPerKey.map(new Function<Tuple2<String, Integer>, Cells>() {
            @Override
            public Cells call(Tuple2<String, Integer> t) {
                Cell c1 = Cell.create("domain",t._1(),true,false);
                Cell c2 = Cell.create("num_pages",t._2());
                return new Cells(c1, c2);
            }
        });

        CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig);

        System.exit(0);
    }
}
