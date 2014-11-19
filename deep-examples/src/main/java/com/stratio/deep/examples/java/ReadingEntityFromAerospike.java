package com.stratio.deep.examples.java;

import com.stratio.deep.aerospike.config.AerospikeConfigFactory;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.java.extractorconfig.mongodb.utils.ContextProperties;
import com.stratio.deep.testentity.MessageEntity;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

/**
 * Example class to read an entity from Aerospike
 */
public class ReadingEntityFromAerospike {

    private static final Logger LOG = Logger.getLogger(com.stratio.deep.examples.java.ReadingEntityFromMongoDB.class);
    public static List<Tuple2<String, Integer>> results;

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:readingEntityFromAerospike";

        String host = "localhost";
        Integer port = 3000;
        String namespace = "test";
        String set = "message";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        AerospikeDeepJobConfig<MessageEntity> inputConfigEntity =
                AerospikeConfigFactory.createAerospike(MessageEntity.class).host(host).port(port).namespace(namespace).set(set).initialize();

        JavaRDD<MessageEntity> inputRDDEntity = deepContext.createJavaRDD(inputConfigEntity);

        LOG.info("count : " + inputRDDEntity.cache().count());

        deepContext.stop();

    }

}
