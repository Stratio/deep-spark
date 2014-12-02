package com.stratio.deep.examples.java.extractorconfig;

import com.stratio.deep.aerospike.extractor.AerospikeCellExtractor;
import com.stratio.deep.aerospike.extractor.AerospikeEntityExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import scala.Tuple2;

import javax.activation.UnsupportedDataTypeException;
import java.util.List;

/**
 * Example for reading Cells and Entities from Aerospike using SparkSQL.
 */
public class ReadingSQLEntityFromAerospike {

    private static final Logger LOG = Logger.getLogger(ReadingSQLEntityFromAerospike.class);
    public static List<Tuple2<String, Integer>> results;

    private ReadingSQLEntityFromAerospike() {

    }

    public static void main(String[] args) throws Exception {
        doMain(args);
    }

    public static void doMain(String[] args) throws UnsupportedDataTypeException {
        String job = "java:readingEntityFromAerospike";

        String host = "localhost";
        int port = 3000;

        String namespace = "test";
        String inputSet = "input";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        ExtractorConfig inputConfigCells = new ExtractorConfig<>(Cells.class);
        inputConfigCells.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.PORT, port).putValue(ExtractorConstants.NAMESPACE, namespace)
                .putValue(ExtractorConstants.SET, inputSet);
        inputConfigCells.setExtractorImplClass(AerospikeCellExtractor.class);

        JavaSchemaRDD schema = deepContext.createJavaSchemaRDD(inputConfigCells);
        schema.registerTempTable("input");

        JavaSchemaRDD messagesFiltered = deepContext.sql("SELECT * FROM input WHERE message = \"message test\" ");

        List<Row> rows = messagesFiltered.collect();

        for(Row row : rows){
            System.out.println(row.get(0));
            System.out.println(row.get(1));
            System.out.println(row.get(2));
        }

        LOG.info("count : " + messagesFiltered.cache().count());
        LOG.info("first : " + messagesFiltered.first());

        JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(deepContext);

        ExtractorConfig<MessageTestEntity> inputConfigEntity = new ExtractorConfig<>(MessageTestEntity.class);
        inputConfigEntity.putValue(ExtractorConstants.HOST, host).putValue(ExtractorConstants.PORT, port).putValue(ExtractorConstants.NAMESPACE, namespace)
                .putValue(ExtractorConstants.SET, inputSet);
        inputConfigEntity.setExtractorImplClass(AerospikeEntityExtractor.class);

        RDD<MessageTestEntity> inputRDDEntity = deepContext.createRDD(inputConfigEntity);

        JavaSchemaRDD schemaEntities = sqlContext.applySchema(inputRDDEntity.toJavaRDD(), MessageTestEntity.class);
        schemaEntities.registerTempTable("input");

        JavaSchemaRDD messagesFilteredEntities = sqlContext.sql("SELECT * FROM input WHERE message != \"message2\" ");

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
