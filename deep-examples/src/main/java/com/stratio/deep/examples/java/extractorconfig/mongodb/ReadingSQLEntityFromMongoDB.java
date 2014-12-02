/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.examples.java.extractorconfig.mongodb;

import java.util.List;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import com.stratio.deep.mongodb.extractor.MongoCellExtractor;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.mongodb.extractor.MongoEntityExtractor;
import com.stratio.deep.utils.ContextProperties;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import javax.activation.UnsupportedDataTypeException;

/**
 * Example class to read an entity from mongoDB
 */
public final class ReadingSQLEntityFromMongoDB {
    private static final Logger LOG = Logger.getLogger(ReadingSQLEntityFromMongoDB.class);
    public static List<Tuple2<String, Integer>> results;

    private ReadingSQLEntityFromMongoDB() {
    }

    public static void main(String[] args) throws Exception {
        doMain(args);
    }

    public static void doMain(String[] args) throws UnsupportedDataTypeException {
        String job = "java:readingEntityFromMongoDB";

        String host = "localhost:27017";

        String database = "test";
        String inputCollection = "prueba";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        ExtractorConfig inputConfigCells = new ExtractorConfig<>(Cells.class);
        inputConfigCells.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, inputCollection);
        inputConfigCells.setExtractorImplClass(MongoCellExtractor.class);

        JavaSchemaRDD schema = deepContext.createJavaSchemaRDD(inputConfigCells);
        schema.registerTempTable("prueba");

        JavaSchemaRDD messagesFiltered = deepContext.sql("SELECT * FROM prueba WHERE message != \"message test\" ");

        List<Row> rows = messagesFiltered.collect();

        for(Row row : rows){
            System.out.println(row.get(0));
            System.out.println(row.get(1));
            System.out.println(row.get(2));
            System.out.println(row.get(3));
        }

        LOG.info("count : " + messagesFiltered.cache().count());
        LOG.info("first : " + messagesFiltered.first());

        JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(deepContext);

        ExtractorConfig<MessageTestEntity> inputConfigEntity = new ExtractorConfig<>(MessageTestEntity.class);
        inputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, inputCollection);
        inputConfigEntity.setExtractorImplClass(MongoEntityExtractor.class);

        RDD<MessageTestEntity> inputRDDEntity = deepContext.createRDD(inputConfigEntity);

        JavaSchemaRDD schemaEntities = sqlContext.applySchema(inputRDDEntity.toJavaRDD(), MessageTestEntity.class);
        schemaEntities.registerTempTable("prueba");

        JavaSchemaRDD messagesFilteredEntities = sqlContext.sql("SELECT * FROM prueba WHERE message != \"message2\" ");

        List<Row> rowsEntities = messagesFilteredEntities.collect();

        for(Row row : rowsEntities){
            System.out.println(row.get(0));
            System.out.println(row.get(1));
        }

        LOG.info("count : " + messagesFilteredEntities.cache().count());
        LOG.info("first : " + messagesFilteredEntities.first());

        LOG.info("count : " + inputRDDEntity.cache().count());
        LOG.info("count : " + inputRDDEntity.first());

        deepContext.stop();

    }
}
