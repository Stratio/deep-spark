package com.stratio.deep.rdd;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.entity.Cql3TestEntity;
import com.stratio.deep.functions.AbstractSerializableFunction;
import com.stratio.deep.util.Constants;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.Test;
import scala.Function1;
import scala.reflect.ClassTag$;

import static org.testng.Assert.*;

/**
 * Created by luca on 03/02/14.
 */
@Test(suiteName = "cassandraRddTests", groups = { "CassandraCql3RDDTest" }, dependsOnGroups = { "CassandraEntityRDDTest" })
public class CassandraCql3RDDTest extends CassandraRDDTest<Cql3TestEntity> {

    private static class TestEntityAbstractSerializableFunction extends
		    AbstractSerializableFunction<Cql3TestEntity, Cql3TestEntity> {
	private static final long serialVersionUID = 6678218192781434399L;

	@Override
	public Cql3TestEntity apply(Cql3TestEntity e) {
	    return new Cql3TestEntity(e.getName(), e.getPassword(), e.getColor(), e.getGender(), e.getFood(),
			    e.getAnimal(), e.getLucene());
	}
    }

    @Override
    protected void checkComputedData(Cql3TestEntity[] entities) {

	boolean found = false;

	assertEquals(entities.length, cql3TestDataSize);

	for (Cql3TestEntity e : entities) {
	    if (e.getName().equals("pepito_3") && e.getAge().equals(-2) && e.getGender().equals("male")
			    && e.getAnimal().equals("monkey")) {
		assertNull(e.getColor());
		assertNull(e.getLucene());
		assertEquals(e.getFood(), "donuts");
		assertEquals(e.getPassword(), "abc");
		found = true;
		break;
	    }
	}

	if (!found) {
	    fail();
	}

    }

    @Test
    public void testAdditionalFilters(){

	CassandraRDD<Cql3TestEntity> otherRDD=initRDD();

	Cql3TestEntity[] entities = (Cql3TestEntity[])otherRDD.collect();

	int allElements = entities.length;
	assertTrue(allElements > 1);
	otherRDD.filterByField("food", "donuts");

	entities = (Cql3TestEntity[])otherRDD.collect();

	assertEquals(entities.length, 1);
	assertEquals(entities[0].getFood(), "donuts");
	assertEquals(entities[0].getName(), "pepito_3");
	assertEquals(entities[0].getGender(), "male");
	assertEquals(entities[0].getAge(), Integer.valueOf(-2));
	assertEquals(entities[0].getAnimal(), "monkey");

	otherRDD.filterByField("food", "chips");
	entities = (Cql3TestEntity[])otherRDD.collect();
	assertEquals(entities.length, allElements-1);

	otherRDD.removeFilterOnField("food");
	entities = (Cql3TestEntity[])otherRDD.collect();
	assertEquals(entities.length, allElements);
    }


    protected void checkOutputTestData() {
	Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
			.addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
	Session session = cluster.connect();

	String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

	ResultSet rs = session.execute(command);
	assertEquals(rs.one().getLong(0), 4);

	command = "SELECT * from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

	rs = session.execute(command);
	for (Row r : rs) {
	    assertEquals(r.getInt("age"), 15);
	}
	session.shutdown();
    }

    @Override
    protected void checkSimpleTestData() {
	Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
			.addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
	Session session = cluster.connect();

	String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

	ResultSet rs = session.execute(command);
	assertEquals(rs.one().getLong(0), cql3TestDataSize);

	command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY
			+ " WHERE name = 'pepito_1' and gender = 'male' and age = 0  and animal = 'monkey';";
	rs = session.execute(command);

	List<Row> rows = rs.all();

	assertNotNull(rows);
	assertEquals(rows.size(), 1);

	Row r = rows.get(0);

	assertEquals(r.getString("password"), "xyz");

	session.shutdown();
    }

    @Override
    protected CassandraRDD<Cql3TestEntity> initRDD() {
	assertNotNull(context);
	return context.cassandraEntityRDD(getReadConfig());
    }

    @Override
    protected IDeepJobConfig<Cql3TestEntity> initReadConfig() {
	return DeepJobConfigFactory.create(Cql3TestEntity.class).host(Constants.DEFAULT_CASSANDRA_HOST)
			.cqlPort(CassandraServer.CASSANDRA_CQL_PORT).rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT).keyspace(KEYSPACE_NAME).columnFamily(CQL3_COLUMN_FAMILY)
			.initialize();
    }

    @Override
    protected IDeepJobConfig<Cql3TestEntity> initWriteConfig() {

	return DeepJobConfigFactory.create(Cql3TestEntity.class).host(Constants.DEFAULT_CASSANDRA_HOST)
			.rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT).keyspace(OUTPUT_KEYSPACE_NAME)
			.cqlPort(CassandraServer.CASSANDRA_CQL_PORT).columnFamily(CQL3_ENTITY_OUTPUT_COLUMN_FAMILY).initialize();
    }

    @Override
    public void testSaveToCassandra() {
	Function1<Cql3TestEntity, Cql3TestEntity> mappingFunc = new TestEntityAbstractSerializableFunction();
	RDD<Cql3TestEntity> mappedRDD = getRDD().map(mappingFunc,
			ClassTag$.MODULE$.<Cql3TestEntity>apply(Cql3TestEntity.class));
	truncateCf(OUTPUT_KEYSPACE_NAME, CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
	assertTrue(mappedRDD.count() > 0);
	CassandraRDD.saveRDDToCassandra(mappedRDD, getWriteConfig());
	checkOutputTestData();
    }

    @Override
    public void testSimpleSaveToCassandra() {
	truncateCf(OUTPUT_KEYSPACE_NAME, CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
	CassandraRDD.saveRDDToCassandra(getRDD(), getWriteConfig());
	checkSimpleTestData();

    }
}
