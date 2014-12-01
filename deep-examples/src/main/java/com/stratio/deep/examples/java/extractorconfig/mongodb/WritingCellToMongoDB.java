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
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.mongodb.extractor.MongoCellExtractor;
import com.stratio.deep.utils.ContextProperties;

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

        ExtractorConfig<Cells> inputConfigEntity = new ExtractorConfig<>();
        inputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, inputCollection);
        inputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);

        RDD<Cells> inputRDDCell = deepContext.createRDD(inputConfigEntity);

        LOG.info("count : " + inputRDDCell.count());
        LOG.info("prints first cell : " + inputRDDCell.first());

        ExtractorConfig<Cells> outputConfigEntity = new ExtractorConfig<>();
        outputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, outputCollection);
        outputConfigEntity.setExtractorImplClass(MongoCellExtractor.class);

        deepContext.saveRDD(inputRDDCell, outputConfigEntity);

        RDD outputRDDCell = deepContext.createRDD(outputConfigEntity);

        LOG.info("count output : " + outputRDDCell.count());
        LOG.info("prints first output cell: " + outputRDDCell.first());

        deepContext.stop();
    }
}
