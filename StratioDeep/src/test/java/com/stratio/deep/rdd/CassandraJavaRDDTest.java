package com.stratio.deep.rdd;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Map;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.context.AbstractDeepSparkContextTest;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.entity.StrippedTestEntity;
import com.stratio.deep.entity.TestEntity;
import com.stratio.deep.util.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
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

@Test(suiteName = "cassandraRddTests", dependsOnGroups = { "CassandraCellRDDTest" })
public final class CassandraJavaRDDTest extends AbstractDeepSparkContextTest {
    private Logger logger = Logger.getLogger(getClass());

    private CassandraJavaRDD<TestEntity> rdd;
    protected IDeepJobConfig<TestEntity> rddConfig;

    JavaRDD<TestEntity> slowPages = null;

    JavaRDD<TestEntity> quickPages = null;

    @BeforeClass
    protected void initServerAndRDD() throws IOException, URISyntaxException, ConfigurationException,
	    InterruptedException {

	rddConfig = DeepJobConfigFactory.create(TestEntity.class).host(Constants.DEFAULT_CASSANDRA_HOST)
			.cqlPort(CassandraServer.CASSANDRA_CQL_PORT).rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT).keyspace(KEYSPACE_NAME).columnFamily(COLUMN_FAMILY)
		.partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
		.serializer("com.stratio.deep.serializer.impl.DefaultDeepSerializer").username("").password("");

	rddConfig.getConfiguration();

	logger.info("Constructed configuration object: " + rddConfig);
	logger.info("Constructiong cassandraRDD");

	rdd = context.cassandraJavaRDD(rddConfig);
    }

    //@Test
    public void testCassandraRDDInstantiation() {
	logger.info("testCassandraRDDInstantiation()");

	assertNotNull(rdd);
    }

    //@Test(dependsOnMethods = "testCassandraRDDInstantiation")
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

    //@Test(dependsOnMethods = "testCollect")
    public void testFilter() {
	slowPages = rdd.filter(new FilterSlowPagesFunction());
	assertEquals(slowPages.count(), 13L);

	quickPages = rdd.filter(new FilterQuickPagesFunction());
	assertEquals(quickPages.count(), 6L);

	logger.info("slowPages:\n" + slowPages.collect());
	logger.info("quickPages:\n" + quickPages.collect());

	slowPages.cache();
	quickPages.cache();
    }

    //@Test(dependsOnMethods = "testFilter")
    public void testGroupByKey() {
	/* 1. I need to define which is the key, let's say it's the domain */
	JavaPairRDD<String, TestEntity> pairRDD = rdd.map(new DomainEntityPairFunction());

	/* 2. Not I can group by domain */
	JavaPairRDD<String, List<TestEntity>> groupedPairRDD = pairRDD.groupByKey();

	assertNotNull(groupedPairRDD);

	Map<String, List<TestEntity>> groupedPairAsMap = groupedPairRDD.collectAsMap();

	assertEquals(groupedPairAsMap.keySet().size(), 8);

	List<TestEntity> domainWickedin = groupedPairAsMap.get("wickedin.es");
	assertEquals(domainWickedin.size(), 2);

	List<TestEntity> domain11870 = groupedPairAsMap.get("11870.com");
	assertEquals(domain11870.size(), 3);

	List<TestEntity> domainAlicanteconfidencial = groupedPairAsMap.get("alicanteconfidencial.blogspot.com.es");
	assertEquals(domainAlicanteconfidencial.size(), 3);
    }

    //@Test(dependsOnMethods = "testFilter")
    public void testJoin() {
	verifyRDD();

	JavaPairRDD<String, TestEntity> slowPagesPairRDD = slowPages.map(new DomainEntityPairFunction());
	JavaPairRDD<String, TestEntity> quickPagesPairRDD = quickPages.map(new DomainEntityPairFunction());

	JavaPairRDD<String, Tuple2<TestEntity, TestEntity>> joinedRDD = slowPagesPairRDD.join(quickPagesPairRDD);
	List<Tuple2<String, Tuple2<TestEntity, TestEntity>>> joinedRDDElems = joinedRDD.collect();

	assertEquals(joinedRDDElems.size(), 4);
    }

    //@Test(dependsOnMethods = "testFilter")
    public void testMap() {
	verifyRDD();

	JavaRDD<StrippedTestEntity> mappedSlowPages = slowPages.map(new StrippedEntityMapFunction());
	JavaRDD<StrippedTestEntity> mappedQuickPages = quickPages.map(new StrippedEntityMapFunction());

	assertEquals(mappedSlowPages.count(), 13L);
	assertEquals(mappedQuickPages.count(), 6L);
    }

    //@Test(dependsOnMethods = "testFilter")
    public void testReduceByKey() {
	/* 1. I need to define which is the key, let's say it's the domain */
	JavaPairRDD<String, Integer> pairRDD = rdd.map(new DomainCounterPairFunction());

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
}

class DomainCounterPairFunction extends PairFunction<TestEntity, String, Integer> {

    private static final long serialVersionUID = -2323312377056863436L;

    @Override
    public Tuple2<String, Integer> call(TestEntity t) throws Exception {
	return new Tuple2<>(t.getDomain(), 1);
    }

}

class DomainEntityPairFunction extends PairFunction<TestEntity, String, TestEntity> {

    private static final long serialVersionUID = -2323312377056863436L;

    @Override
    public Tuple2<String, TestEntity> call(TestEntity t) throws Exception {
	return new Tuple2<>(t.getDomain(), t);
    }

}

class FilterQuickPagesFunction extends org.apache.spark.api.java.function.Function<TestEntity, Boolean> {

    private static final long serialVersionUID = -3435107153980765475L;

    @Override
    public Boolean call(TestEntity t) throws Exception {
	return t.getResponseTime() < 500;
    }
}

class FilterSlowPagesFunction extends org.apache.spark.api.java.function.Function<TestEntity, Boolean> {

    private static final long serialVersionUID = -3435107153980765475L;

    @Override
    public Boolean call(TestEntity t) throws Exception {
	return t.getResponseTime() >= 500 /* (ms) returns only those pages that took very long to be downloaded */;
    }
}

class IntegerReducer extends Function2<Integer, Integer, Integer> {

    private static final long serialVersionUID = 1997328240613872736L;

    @Override
    public Integer call(Integer t1, Integer t2) throws Exception {
	return t1 + t2;
    }

}

class StrippedEntityMapFunction extends Function<TestEntity, StrippedTestEntity> {

    private static final long serialVersionUID = 1L;

    @Override
    public StrippedTestEntity call(TestEntity t) throws Exception {
	return new StrippedTestEntity(t);
    }
}