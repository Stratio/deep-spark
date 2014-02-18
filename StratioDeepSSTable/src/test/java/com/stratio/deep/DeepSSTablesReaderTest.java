package com.stratio.deep;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeepSSTablesReaderTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testReadData() {
	System.setProperty("cassandra.config", "file:///Users/luca/Java/apache-cassandra-2.0.2/conf/cassandra.yaml");

	try {
	    DeepSSTablesReader.readData("crawler_keyspace");

	} catch (IOException e) {
	    e.printStackTrace();

	    fail("Not yet implemented");
	}
    }

}
