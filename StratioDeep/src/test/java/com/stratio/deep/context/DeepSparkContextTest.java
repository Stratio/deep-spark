package com.stratio.deep.context;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.annotations.Test;

/**
 * Created by luca on 28/02/14.
 */
@Test
public class DeepSparkContextTest {

    public void testInstantiationBySparkContext(){
	SparkContext sc = new SparkContext("local", "myapp1", new SparkConf());
	new DeepSparkContext(sc);

    }

    public void testInstantiationWithJar(){
	new DeepSparkContext("local", "myapp1", "/tmp", "");

    }


    public void testInstantiationWithJars(){
	new DeepSparkContext("local", "myapp1", "/tmp", new String[]{"",""});

    }

    public void testInstantiationWithJarsAndEnv(){
	Map<String, String> env = new HashMap<>();
	new DeepSparkContext("local", "myapp1", "/tmp", new String[]{"",""}, env);
    }
}
