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

package com.stratio.deep.examples.java.extractorconfig.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.hdfs.utils.HdfsOutputFormat;
import com.stratio.deep.core.hdfs.utils.MapSchemaFromLines;
import com.stratio.deep.core.hdfs.utils.SchemaMap;
import com.stratio.deep.core.hdfs.utils.TableMap;
import com.stratio.deep.core.hdfs.utils.TableName;
import com.stratio.deep.examples.java.extractorconfig.hdfs.utils.ContextProperties;

import scala.Tuple2;

public class AggregatingData {
    private static final Logger LOG = Logger.getLogger(AggregatingData.class);

    /* used to perform external tests */
    private static Double avg;
    private static Double variance;
    private static Double stddev;
    private static Double count;

    private AggregatingData() {
    }

    /**
     * Application entry point.
     *
     * @param args the arguments passed to the application.
     */
    public static void main(String[] args) throws IOException {
        doMain(args);
    }

    /**
     * This is the method called by both main and tests.
     *
     * @param args
     */
    public static void doMain(String[] args)  {
        String job = "java:aggregatingDataHDFS";

        final String keyspaceName = "test";
        final String tableName    = "songs";
        String home = System.getProperty("hadoop.home.dir");
        final String  splitSep = ",";


        // fall back to the system/user-global env variable
        final ArrayList<SchemaMap<?>> listSchemaMap = new ArrayList<>();
        listSchemaMap.add(new SchemaMap("id",    String.class));
        listSchemaMap.add(new SchemaMap("author",String.class));
        listSchemaMap.add(new SchemaMap("Title", String.class));
        listSchemaMap.add(new SchemaMap("Year",  Integer.class));
        listSchemaMap.add(new SchemaMap("Length",Integer.class));
        listSchemaMap.add(new SchemaMap("Single",String.class));

        TableMap tableMap = new TableMap(new TableName(keyspaceName,tableName),listSchemaMap);
        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
	    DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        // Creating the RDD
        JavaRDD<String> rdd = deepContext.textFile("hdfs://127.0.0.1:9000/user/hadoop/test/songs.csv");


        JavaRDD<Cells> resultCells = rdd.map(new MapSchemaFromLines(tableMap, splitSep));

        resultCells.collect();
        long numAsElvis = rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("Elvis"); }
        }).count();

        JavaRDD<String> lines = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) { return Arrays.asList(x.split("\n")); }
        });
        //words.count();
        JavaPairRDD<String, Integer> result = lines.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) { return new Tuple2(x, 1); }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                });

        result.first();
        result.count();



        //TEST write over specific file name
        Configuration conf = new Configuration();
        conf.addResource("songs.csv");

        HdfsOutputFormat<LongWritable, Text> textOutputFormat= new HdfsOutputFormat<>();
        textOutputFormat.setFileName("song.csv");


        TextOutputFormat<LongWritable, Text> textOutputFormat2= new TextOutputFormat<>();
        JobConf jobConfig = new JobConf();
        jobConfig.set(org.apache.hadoop.mapreduce.lib.output.
                FileOutputFormat.OUTDIR, "hdfs://127.0.0.1:9000/user/hadoop/test/songs/songs.csv");

        textOutputFormat.setOutputPath(jobConfig, new org.apache.hadoop.fs.Path("hdfs://127.0.0.1:9000/user/hadoop/test/songs/songs.csv"));


        result.saveAsHadoopFile("hdfs://127.0.0.1:9000/user/hadoop/test/songs",
                LongWritable.class, Text.class, textOutputFormat2.getClass(), jobConfig);

        JavaRDD<String> file = deepContext.textFile("hdfs://127.0.0.1:9000/user/hadoop/test/songs");


        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(",")); }
        });
        words.collect();

        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });

        words.repartition(1).saveAsTextFile("hdfs://127.0.0.1:9000/user/hadoop/test/save1");

        String resultFirst = rdd.first();

        deepContext.stop();
    }

    public static Double getAvg() {
        return avg;
    }

    public static Double getVariance() {
        return variance;
    }

    public static Double getStddev() {
        return stddev;
    }

    public static Double getCount() {
        return count;
    }
}
