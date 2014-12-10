package com.stratio.deep.examples.java;

import com.stratio.deep.aerospike.config.AerospikeConfigFactory;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.jdbc.config.JdbcConfigFactory;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

/**
 * Created by mariomgal on 10/12/14.
 */
public class ReadingCellsFromJdbc {

    private static final Logger LOG = Logger.getLogger(ReadingCellsFromJdbc.class);

    private ReadingCellsFromJdbc() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:readingCellFromJdbc";

        String host = "127.0.0.1";
        int port = 3306;

        String schema = "deeptest";
        String table = "employee";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        JdbcDeepJobConfig inputConfigCell = JdbcConfigFactory.createJdbc().host(host).port(port)
                .database(schema)
                .table(table)
                .username("root")
                .password("root")
                .query("select * from employee");

        RDD inputRDDCell = deepContext.createRDD(inputConfigCell);

        LOG.info("count : " + inputRDDCell.count());
        LOG.info("prints first cell  : " + inputRDDCell.first());

        deepContext.stop();

    }
}
