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

package com.stratio.deep.cassandra.rdd;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.context.AbstractDeepSparkContextTest;
import com.stratio.deep.cassandra.embedded.CassandraServer;
import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.cassandra.extractor.CassandraEntityExtractor;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepNoSuchFieldException;
import com.stratio.deep.cassandra.testentity.StrippedTestEntity;
import com.stratio.deep.cassandra.testentity.TestEntity;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.utils.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Tuple2;

import static org.testng.Assert.*;

/**
 * Integration tests for Java RDDs.
 */
@Test(suiteName = "cassandraRddTests", dependsOnGroups = {"CassandraCellRDDTest"}, groups = {"CassandraJavaRDDTest"})
public final class CassandraJavaRDDTest extends AbstractDeepSparkContextTest {
    private Logger logger = Logger.getLogger(getClass());

    private JavaRDD<TestEntity> rdd;
    protected ExtractorConfig<TestEntity> rddConfig;

    JavaRDD<TestEntity> slowPages = null;
    JavaRDD<TestEntity> quickPages = null;

    @BeforeClass
    protected void initServerAndRDD() throws IOException, URISyntaxException, ConfigurationException,
            InterruptedException {



        rddConfig = new ExtractorConfig<>(TestEntity.class);
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, COLUMN_FAMILY);
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
//        values.put(ExtractorConstants.P, "org.apache.cassandra.dht.Murmur3Partitioner")

        rddConfig.setValues(values);
        rddConfig.setExtractorImplClass(CassandraEntityExtractor.class);



        logger.info("Constructed configuration object: " + rddConfig);
        logger.info("Constructiong cassandraRDD");

        rdd = (JavaRDD<TestEntity>) context.createJavaRDD(rddConfig);
    }

    @Test
    public void testCassandraRDDInstantiation() {
        logger.info("testCassandraRDDInstantiation()");

        assertNotNull(rdd);
    }

    @Test(dependsOnMethods = "testCassandraRDDInstantiation")
    public void testCollect() throws CharacterCodingException {
        logger.info("testCollect()");
        long count = rdd.count();
        assertEquals(count, entityTestDataSize);

        List<TestEntity> entities = rdd.collect();

        assertNotNull(entities);

        boolean found = false;

        for (TestEntity e : entities) {
            if (e.getId().equals("e71aa3103bb4a63b9e7d3aa081c1dc5ddef85fa7")) {
                assertEquals(e.getUrl(), "http://11870.com/k/es/de");
                assertEquals(e.getResponseTime(), new Integer(421));
                assertEquals(e.getDownloadTime(), new Long(1380802049275L));
                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }
    }

    @Test(dependsOnMethods = "testCollect")
    public void testFilter() {
        slowPages = rdd.filter(new FilterSlowPagesFunction());
        assertEquals(slowPages.count(), 13L);

        quickPages = rdd.filter(new FilterQuickPagesFunction());
        assertEquals(quickPages.count(), 6L);

        slowPages.cache();
        quickPages.cache();
    }

    @Test(dependsOnMethods = "testFilter")
    public void testGroupByKey() {
        /* 1. I need to define which is the key, let's say it's the domain */
        JavaPairRDD<String, TestEntity> pairRDD = rdd.mapToPair(new DomainEntityPairFunction());

	    /* 2. Not I can group by domain */
        JavaPairRDD<String, Iterable<TestEntity>> groupedPairRDD = pairRDD.groupByKey();

        assertNotNull(groupedPairRDD);

        Map<String, Iterable<TestEntity>> groupedPairAsMap = groupedPairRDD.collectAsMap();

        assertEquals(groupedPairAsMap.keySet().size(), 8);

        Iterable<TestEntity> domainWickedin = groupedPairAsMap.get("wickedin.es");
        assertEquals(Lists.newArrayList(domainWickedin).size(), 2);

        Iterable<TestEntity> domain11870 = groupedPairAsMap.get("11870.com");
        assertEquals(Lists.newArrayList(domain11870).size(), 3);

        Iterable<TestEntity> domainAlicanteconfidencial = groupedPairAsMap.get("alicanteconfidencial.blogspot.com.es");
        assertEquals(Lists.newArrayList(domainAlicanteconfidencial).size(), 3);
    }

    @Test(dependsOnMethods = "testGroupByKey")
    public void testJoin() {
        verifyRDD();

        JavaPairRDD<String, TestEntity> slowPagesPairRDD = slowPages.mapToPair(new DomainEntityPairFunction());
        JavaPairRDD<String, TestEntity> quickPagesPairRDD = quickPages.mapToPair(new DomainEntityPairFunction());

        JavaPairRDD<String, Tuple2<TestEntity, TestEntity>> joinedRDD = slowPagesPairRDD.join(quickPagesPairRDD);
        List<Tuple2<String, Tuple2<TestEntity, TestEntity>>> joinedRDDElems = joinedRDD.collect();

        assertEquals(joinedRDDElems.size(), 4);
    }

    @Test(dependsOnMethods = "testJoin")
    public void testMap() {
        verifyRDD();

        JavaRDD<StrippedTestEntity> mappedSlowPages = slowPages.map(new StrippedEntityMapFunction());
        JavaRDD<StrippedTestEntity> mappedQuickPages = quickPages.map(new StrippedEntityMapFunction());

        assertEquals(mappedSlowPages.count(), 13L);
        assertEquals(mappedQuickPages.count(), 6L);
    }

    @Test(dependsOnMethods = "testMap")
    public void testReduceByKey() {
  /* 1. I need to define which is the key, let's say it's the domain */
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new DomainCounterPairFunction());

	/* 2. Not I can group by domain */
        JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey(new IntegerReducer());

        assertNotNull(reducedRDD);

        Map<String, Integer> groupedPairAsMap = reducedRDD.collectAsMap();
        assertEquals(groupedPairAsMap.keySet().size(), 8);

        Integer domainWickedin = groupedPairAsMap.get("wickedin.es");
        assertEquals(domainWickedin.intValue(), 2);

        Integer domain11870 = groupedPairAsMap.get("11870.com");
        assertEquals(domain11870.intValue(), 3);

        Integer domainAlicanteconfidencial = groupedPairAsMap.get("alicanteconfidencial.blogspot.com.es");
        assertEquals(domainAlicanteconfidencial.intValue(), 3);

        Integer domainAboutWickedin = groupedPairAsMap.get("about.wickedin.es");
        assertEquals(domainAboutWickedin.intValue(), 1);

        Integer domainActualidades = groupedPairAsMap.get("actualidades.es");
        assertEquals(domainActualidades.intValue(), 3);
    }

    private void verifyRDD() {
        assertNotNull(slowPages);
        assertNotNull(quickPages);
    }

    @Test(dependsOnMethods = "testReduceByKey")
    public void testSaveToCassandra() {
        final String table = "save_java_rdd";

        //executeCustomCQL("create table  " + OUTPUT_KEYSPACE_NAME + "." + table + " (domain text, count int,
        // PRIMARY KEY(domain));");


        ExtractorConfig<Cells> writeConfig = new ExtractorConfig<>();
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, OUTPUT_KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, table);
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.CREATE_ON_WRITE, "true");

        writeConfig.setValues(values);
        writeConfig.setExtractorImplClass(CassandraCellExtractor.class);




	/* 1. I need to define which is the key, let's say it's the domain */
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new DomainCounterPairFunction());

	/* 2. Not I can group by domain */
        JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey(new IntegerReducer());

        JavaRDD<Cells> cells = reducedRDD
                .map(new Tuple2CellsFunction());

        context.saveRDD(cells.rdd(), writeConfig);

        checkOutputTestData();

    }

    @Test(dependsOnMethods = "testSaveToCassandra")
    public void testSaveToCassandra2() {


        ExtractorConfig<Cells> writeConfig = new ExtractorConfig<>();
        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST, Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.KEYSPACE, OUTPUT_KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY, "page");
        values.put(ExtractorConstants.CQLPORT, String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.CREATE_ON_WRITE, "true");

        writeConfig.setValues(values);
        writeConfig.setExtractorImplClass(CassandraCellExtractor.class);

        JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new DomainCounterDoublePairFunction());

        JavaRDD<Cells> outRDD = pairRDD.map(new WrongSensors2CellsFunction());

        try {
            context.saveRDD(outRDD.rdd(), writeConfig);

            fail();
        } catch (DeepNoSuchFieldException e) {
            // OK
            logger.info("Correctly catched DeepNoSuchFieldException");
        }

        outRDD = pairRDD.map(new Sensors2CellsFunction());
        context.saveRDD(outRDD.rdd(), writeConfig);
    }

    private void checkOutputTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + ".save_java_rdd;";
        ResultSet rs = session.execute(command);

        assertEquals(rs.one().getLong(0), 8);
        command = "select * from " + OUTPUT_KEYSPACE_NAME + ".save_java_rdd;";

        rs = session.execute(command);

        for (Row r : rs) {
            switch (r.getString("domain")) {
                case "wickedin.es":
                    assertEquals(r.getInt("count"), 2);
                    break;
                case "11870.com":
                    assertEquals(r.getInt("count"), 3);
                    break;
                case "alicanteconfidencial.blogspot.com.es":
                    assertEquals(r.getInt("count"), 3);
                    break;
                case "about.wickedin.es":
                    assertEquals(r.getInt("count"), 1);
                    break;
                case "actualidades.es":
                    assertEquals(r.getInt("count"), 3);
                    break;
                case "ahorromovil.wordpress.com":
                    assertEquals(r.getInt("count"), 3);
                    break;
                case "airscoop-espana.blogspot.com.es":
                    assertEquals(r.getInt("count"), 3);
                    break;
                case "america.infobae.com":
                    assertEquals(r.getInt("count"), 1);
                    break;
                default:
                    fail();
            }
        }
        session.close();
    }


}

class WrongSensors2CellsFunction implements Function<Tuple2<String, Double>, Cells> {
    @Override
    public Cells call(Tuple2<String, Double> t) throws Exception {
        Cell sensorNameCell = CassandraCell.create("name", t._1());
        Cell sensorTimeUUID = CassandraCell.create("time_taken", UUIDGen.getTimeUUID());
        Cell sensorDataCell = CassandraCell.create("value", t._2());

        return new Cells("defaultTable",sensorNameCell, sensorTimeUUID, sensorDataCell);
    }
}

class Sensors2CellsFunction implements Function<Tuple2<String, Double>, Cells> {
    @Override
    public Cells call(Tuple2<String, Double> t) throws Exception {
        Cell sensorNameCell = CassandraCell.create("name", t._1());
        Cell sensorTimeUUID = CassandraCell.create("time_taken", UUIDGen.getTimeUUID(), true, false);
        Cell sensorDataCell = CassandraCell.create("value", t._2());

        return new Cells("defaultTable",sensorNameCell, sensorTimeUUID, sensorDataCell);
    }
}

class Tuple2CellsFunction implements Function<Tuple2<String, Integer>, Cells> {
    @Override
    public Cells call(Tuple2<String, Integer> t) throws Exception {
        return new Cells("defaultTable", CassandraCell.create("domain", t._1(), true, false), CassandraCell.create("count", t._2()));
    }
}

class DomainCounterPairFunction implements PairFunction<TestEntity, String, Integer> {

    private static final long serialVersionUID = -2323312377056863436L;

    @Override
    public Tuple2<String, Integer> call(TestEntity t) throws Exception {
        return new Tuple2<>(t.getDomain(), 1);
    }

}

class DomainCounterDoublePairFunction implements PairFunction<TestEntity, String, Double> {

    private static final long serialVersionUID = -2323312377056863436L;

    @Override
    public Tuple2<String, Double> call(TestEntity t) throws Exception {
        return new Tuple2<>(t.getDomain(), new Double(1));
    }

}

class DomainEntityPairFunction implements PairFunction<TestEntity, String, TestEntity> {

    private static final long serialVersionUID = -2323312377056863436L;

    @Override
    public Tuple2<String, TestEntity> call(TestEntity t) throws Exception {
        return new Tuple2<>(t.getDomain(), t);
    }

}

class FilterQuickPagesFunction implements org.apache.spark.api.java.function.Function<TestEntity, Boolean> {

    private static final long serialVersionUID = -3435107153980765475L;

    @Override
    public Boolean call(TestEntity t) throws Exception {
        return t.getResponseTime() < 500;
    }
}

class FilterSlowPagesFunction implements org.apache.spark.api.java.function.Function<TestEntity, Boolean> {

    private static final long serialVersionUID = -3435107153980765475L;

    @Override
    public Boolean call(TestEntity t) throws Exception {
        return t.getResponseTime() >= 500 /* (ms) returns only those pages that took very long to be downloaded */;
    }
}

class IntegerReducer implements Function2<Integer, Integer, Integer> {

    private static final long serialVersionUID = 1997328240613872736L;

    @Override
    public Integer call(Integer t1, Integer t2) throws Exception {
        return t1 + t2;
    }

}

class StrippedEntityMapFunction implements Function<TestEntity, StrippedTestEntity> {

    private static final long serialVersionUID = 1L;

    @Override
    public StrippedTestEntity call(TestEntity t) throws Exception {
        return new StrippedTestEntity(t);
    }
}
