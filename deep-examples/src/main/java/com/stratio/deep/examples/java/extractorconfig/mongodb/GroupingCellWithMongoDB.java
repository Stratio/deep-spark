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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.mongodb.extractor.MongoCellExtractor;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.utils.ContextProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

/**
 * Created by rcrespo on 25/06/14.
 */
public final class GroupingCellWithMongoDB {

    private GroupingCellWithMongoDB() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "scala:groupingCellWithMongoDB";

        String host = "localhost:27017";

        String database = "test";
        String inputCollection = "input";
        String outputCollection = "output";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        ExtractorConfig<Cells> inputConfigEntity = new ExtractorConfig<>();
        inputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, inputCollection);
        inputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);

        RDD<Cells> inputRDDEntity = deepContext.createRDD(inputConfigEntity);


        JavaPairRDD<Double, String> a1 = inputRDDEntity.toJavaRDD().mapToPair(new PairFunction<Cells, Double,
                String>() {
            @Override
            public Tuple2<Double, String> call(Cells cells) throws Exception {
                return new Tuple2(cells.getCellByName("number").getDouble(), cells.getCellByName("text").getString());
            }
        });

        JavaPairRDD<Double, String> a2 = inputRDDEntity.toJavaRDD().mapToPair(new PairFunction<Cells, Double,
                String>() {
            @Override
            public Tuple2<Double, String> call(Cells cells) throws Exception {
                return new Tuple2(cells.getCellByName("number").getDouble(), cells.getCellByName("text").getString());
            }
        });


        JavaPairRDD<Double, Tuple2<String, String>> c4= a1.join(a2);

        JavaRDD<Cells> cells = c4.map(new Function<Tuple2<Double, Tuple2<String, String>>, Cells>() {
            @Override
            public Cells call(Tuple2<Double, Tuple2<String, String>> v1) throws Exception {
                return new Cells(Cell.create("marca", v1._1()), Cell.create( "campo1",v1._2()._1()),
                        Cell.create( "campo2",v1._2()._2()) );
            }
        });

        System.out.println("imprimo el join "+cells.first());


//        deepContext.saveRDD(outputRDD.rdd(), outputConfigEntity);

        deepContext.stop();

    }

}
