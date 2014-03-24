/*
Copyright 2014 Stratio.

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.
*/
package com.stratio.deep.examples.java;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.rdd.CassandraJavaRDD;

// !!Important!!
import com.stratio.deep.utils.ContextProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.List;

import com.stratio.deep.entity.TweetEntity;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Author: Emmanuelle Raffenne
 * Date..: 13-feb-2014
 */
public class GroupingByColumn {

    public static void main(String[] args) {

        String job = "java:groupingByColumn";

        String keyspaceName = "tutorials";
        String tableName = "tweets";


        // Creating the Deep Context
        ContextProperties p = new ContextProperties();
        DeepSparkContext deepContext = new DeepSparkContext(p.cluster, job, p.sparkHome, p.jarList);

// Create a configuration for the RDD and initialize it
        IDeepJobConfig config = DeepJobConfigFactory.create(TweetEntity.class)
                .host(p.cassandraHost).rpcPort(p.cassandraPort)
                .keyspace(keyspaceName).table(tableName)
                .initialize();

// Creating the RDD
        CassandraJavaRDD rdd = deepContext.cassandraJavaRDD(config);

        // grouping
        JavaPairRDD<String,List<TweetEntity>> groups = rdd.groupBy(new Function<TweetEntity, String>() {
            @Override
            public String call(TweetEntity tableEntity) throws Exception {
                return tableEntity.getAuthor();
            }
        });

// counting elements in groups
        JavaPairRDD<String,Integer> counts = groups.map(new PairFunction<Tuple2<String, List<TweetEntity>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, List<TweetEntity>> t) throws Exception {
                return new Tuple2<String,Integer>(t._1(), t._2().size());
            }
        });

// fetching the results
        List<Tuple2<String,Integer>> results = counts.collect();

        System.out.println("Este es el resultado con groupBy: ");
        for (Tuple2 t : results) {
            System.out.println(t._1() + ": " + t._2().toString());
        }

        System.exit(0);
    }
}
