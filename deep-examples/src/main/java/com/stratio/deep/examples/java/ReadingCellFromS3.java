/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.stratio.deep.examples.java;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.fs.utils.SchemaMap;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;

/**
 * Created by mariomgal on 04/12/14.
 */
public class ReadingCellFromS3 {

    private static final Logger LOG = Logger.getLogger(ReadingCellFromS3.class);

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {

        String job = "java:readingCellFromS3";
        final String keyspaceName = "test";
        final String tableName = "people";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        ArrayList<SchemaMap> listSchemaMap = new ArrayList<>();
        listSchemaMap.add(new SchemaMap("name", String.class));
        listSchemaMap.add(new SchemaMap("last", String.class));
        listSchemaMap.add(new SchemaMap("age", String.class));

        ExtractorConfig inputConfigCells = new ExtractorConfig<>(Cells.class);
        inputConfigCells.putValue(ExtractorConstants.FS_FILE_SEPARATOR, ",")
                .putValue(ExtractorConstants.S3_TYPE, ExtractorConstants.S3_TYPE)
                .putValue(ExtractorConstants.S3_BUCKET, "<YOUR S3 BUCKET>")
                .putValue(ExtractorConstants.FS_SCHEMA, listSchemaMap)
                .putValue(ExtractorConstants.FS_FILE_PATH, "<YOUR S3 FILE PATH>")
                .putValue(ExtractorConstants.S3_ACCESS_KEY_ID, "<YOUR S3 ACCESS KEY>")
                .putValue(ExtractorConstants.S3_SECRET_ACCESS_KEY, "<YOUR S3 SECRET ACCESS KEY>")
                .putValue(ExtractorConstants.TABLE, tableName)
                .putValue(ExtractorConstants.CATALOG, keyspaceName);
        inputConfigCells.setExtractorImplClassName(ExtractorConstants.S3);

        RDD inputRDDCell = deepContext.textFile(inputConfigCells);
        LOG.info("count : " + inputRDDCell.count());
        LOG.info("prints first cell  : " + inputRDDCell.first());

        deepContext.stop();

    }
}
