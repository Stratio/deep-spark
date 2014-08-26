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
import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.entity.CassandraCell;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.extractor.server.ExtractorServer;
import com.stratio.deep.rdd.CassandraCellExtractor;
import com.stratio.deep.rdd.CassandraExtractor;

import java.util.List;



import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 3-mar-2014
 */
public final class WritingCellToCassandra {
    private static final Logger LOG = Logger.getLogger(WritingCellToCassandra.class);
    public static List<Tuple2<String, Integer>> results;

    private WritingCellToCassandra() {
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
        String job = "java:writingCellToCassandra";

        String keyspaceName = "crawler";
        String inputTableName = "Page";
        final String outputTableName = "newlistdomains";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
	    DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        // --- INPUT RDD
        ExtractorConfig<Cells> inputConfig = new ExtractorConfig();

        RDD<Cells> inputRDD = deepContext.createRDD(inputConfig);

        JavaPairRDD<String, Cells> pairRDD = inputRDD.toJavaRDD().mapToPair(new PairFunction<Cells, String, Cells>() {
            @Override
            public Tuple2<String, Cells> call(Cells c) {
                return new Tuple2<String, Cells>((String) c.getCellByName("domainName")
                        .getCellValue(), c);
            }
        });

        JavaPairRDD<String, Integer> numPerKey = pairRDD.groupByKey()
                .mapToPair(new PairFunction<Tuple2<String, Iterable<Cells>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Iterable<Cells>> t) {
                        return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
                    }
                });

        results = numPerKey.collect();

        for (Tuple2<String, Integer> result : results) {
            LOG.info(result);
        }


        // --- OUTPUT RDD
        ExtractorConfig<Cells> outputConfig = new ExtractorConfig();


        JavaRDD<Cells> outputRDD = numPerKey.map(new Function<Tuple2<String, Integer>, Cells>() {
            @Override
            public Cells call(Tuple2<String, Integer> t) {
                Cell c1 = CassandraCell.create("domain", t._1(), true, false);
                Cell c2 = CassandraCell.create("num_pages", t._2());
                return new Cells(outputTableName,c1, c2);
            }
        });

        //CassandraRDD.saveRDDToCassandra(outputRDD, outputConfig);

        deepContext.stop();
    }
}
