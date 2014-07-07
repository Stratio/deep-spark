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

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.config.IMongoDeepJobConfig;
import com.stratio.deep.context.DeepSparkContext;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.mongodb.MongoCellRDD;
import com.stratio.deep.rdd.mongodb.MongoJavaRDD;
import com.stratio.deep.testutils.ContextProperties;
import org.apache.log4j.Logger;

/**
 * Example class to write a RDD to mongoDB
 */
public final class WritingCellToMongoDB {
    private static final Logger LOG = Logger.getLogger(WritingCellToMongoDB.class);

    private WritingCellToMongoDB() {
    }


    public static void main(String[] args) {
        doMain(args);
    }


    public static void doMain(String[] args) {
        String job = "java:writingCellToMongoDB";

        String host = "localhost:27017";

        String database = "test";
        String inputCollection = "input";

        String outputCollection = "output";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());


        IMongoDeepJobConfig inputConfigEntity = DeepJobConfigFactory.createMongoDB().host(host).database(database).collection(inputCollection).initialize();

        MongoCellRDD inputRDDCell = deepContext.mongoCellRDD(inputConfigEntity);


        System.out.println("count : " + inputRDDCell.count());


        System.out.println("prints first cell : " + inputRDDCell.first());

        IMongoDeepJobConfig outputConfigEntity = DeepJobConfigFactory.createMongoDB().host(host).database(database).collection(outputCollection).initialize();


        MongoCellRDD.saveCell(inputRDDCell, outputConfigEntity);


        MongoCellRDD outputRDDCell = deepContext.mongoCellRDD(outputConfigEntity);

        System.out.println("count output : " + outputRDDCell.count());

        System.out.println("prints first output cell: " + outputRDDCell.first());

        deepContext.stop();
    }
}
