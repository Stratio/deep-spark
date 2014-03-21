package com.stratio.deep.context;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.annotations.Test;

/**
 * Created by luca on 28/02/14.
 */
@Test(suiteName = "DeepSparkContextTest")
public class DeepSparkContextTest {

    public void testInstantiationBySparkContext() {
        DeepSparkContext sc = new DeepSparkContext(new SparkContext("local", "myapp1", new SparkConf()));

        sc.stop();
    }

    public void testInstantiationWithJar() {
        DeepSparkContext sc = new DeepSparkContext("local", "myapp1", "/tmp", "");
        sc.stop();
    }


    public void testInstantiationWithJars() {
        DeepSparkContext sc = new DeepSparkContext("local", "myapp1", "/tmp", new String[] {"", ""});
        sc.stop();
    }

    public void testInstantiationWithJarsAndEnv() {
        Map<String, String> env = new HashMap<>();
        DeepSparkContext sc = new DeepSparkContext("local", "myapp1", "/tmp", new String[] {"", ""}, env);
        sc.stop();
    }
}
