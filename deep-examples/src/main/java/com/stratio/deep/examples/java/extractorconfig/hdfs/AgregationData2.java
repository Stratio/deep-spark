package com.stratio.deep.examples.java.extractorconfig.hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.rdd.RDD;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.hdfs.utils.SchemaMap;
import com.stratio.deep.examples.java.extractorconfig.hdfs.utils.ContextProperties;


public class AgregationData2 {

    /**
     * Application entry point.
     *
     * @param args the arguments passed to the application.
     */
    public static void main(String[] args) {
        doMain(args);
    }

    /**
     * This is the method called by both main and tests.
     *
     * @param args
     */
    public static void doMain(String[] args) {
        String job = "java:aggregatingDataHDFS";

        final String keyspaceName = "test";
        final String tableName    = "songs";
        String home = System.getProperty("hadoop.home.dir");
        final String  splitSep = ",";


        // fall back to the system/user-global env variable

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        ExtractorConfig<Cells> extractorConfig = new ExtractorConfig<>();

        Map<String, Serializable> values = new HashMap<>();

        ArrayList<SchemaMap> listSchemaMap = new ArrayList<>();
        listSchemaMap.add(new SchemaMap("id",    String.class));
        listSchemaMap.add(new SchemaMap("author",String.class));
        listSchemaMap.add(new SchemaMap("Title", String.class));
        listSchemaMap.add(new SchemaMap("Year",  Integer.class));
        listSchemaMap.add(new SchemaMap("Length",Integer.class));
        listSchemaMap.add(new SchemaMap("Single",String.class));

        values.put(ExtractorConstants.PORT, "9000");
        values.put("separator", ",");
        values.put("path", "user/hadoop/test/songs.csv");
        values.put(ExtractorConstants.HOST, "127.0.0.1");
        values.put("map",listSchemaMap);
        values.put("type","hdfsType");

        extractorConfig.setValues(values);

        // Creating the RDD
        RDD<Cells> rdd = deepContext.createHDFSRDD(extractorConfig);

        rdd.collect();
        deepContext.stop();
    }
}
