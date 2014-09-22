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

package com.stratio.deep.cassandra.utils;

import java.util.Date;
import java.util.UUID;

import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.testentity.CommonsTestEntity;
import org.testng.annotations.Test;
import scala.Tuple2;


import static com.stratio.deep.cassandra.util.CassandraUtils.createTableQueryGenerator;
import static com.stratio.deep.cassandra.util.CassandraUtils.deepType2tuple;
import static com.stratio.deep.cassandra.util.CassandraUtils.updateQueryGenerator;
import static com.stratio.deep.commons.utils.Utils.prepareTuple4CqlDriver;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

/**
 * Created by luca on 11/07/14.
 */
public class CassandraRDDUtilsTest {
	private static final String OUTPUT_KEYSPACE_NAME = "out_test_keyspace";
	private static final String OUTPUT_COLUMN_FAMILY = "out_test_page";

	@Test
	public void testDeepType2Pair() {

		CommonsTestEntity te = new CommonsTestEntity();
		te.setDomain("abc.es");
		te.setId("43274632");
		te.setResponseCode(312);

		Tuple2<Cells, Cells> pair = deepType2tuple(te);

		assertNotNull(pair);
		assertNotNull(pair._1());
		assertNotNull(pair._2());
		assertEquals(pair._1().size(), 1);
		assertEquals(pair._2().size(), 8);

		assertEquals(pair._2().getCellByName("response_code").getCellValue(), 312);
		assertEquals(pair._2().getCellByName("domain_name").getCellValue(), "abc.es");

		assertEquals(pair._1().getCellByName("id").getCellValue(), "43274632");

	}

	@Test
	public void testUpdateQueryGenerator() {
		Cells keys = new Cells("defaultTable",
						CassandraCell.create("id1", "", true, false), CassandraCell.create("id2", "", true, false));

		Cells values = new Cells("defaultTable",CassandraCell.create("domain_name", ""), CassandraCell.create("url", ""), CassandraCell.create("response_time",
						""), CassandraCell.create("response_code", ""), CassandraCell.create("download_time", ""));

		String sql = updateQueryGenerator(keys, values, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);

		assertEquals(
						sql,
						"UPDATE "
										+ OUTPUT_KEYSPACE_NAME
										+ "."
										+ OUTPUT_COLUMN_FAMILY
										+ " SET \"domain_name\" = ?, \"url\" = ?, \"response_time\" = ?, \"response_code\" = ?, " +
										"\"download_time\" = ? WHERE \"id1\" = ? AND \"id2\" = ?;"
		);

	}

	@Test
	public void testCreateTableQueryGeneratorComposite() {

		try {

			createTableQueryGenerator(null, null, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);
			fail();
		} catch (DeepGenericException e) {
			// ok
		}

		UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");

		Cells keys = new Cells("defaultTable",CassandraCell.create("id1", "", true, false),
						CassandraCell.create("id2", testTimeUUID, true, false),
						CassandraCell.create("id3", new Integer(0), false, true));

		Cells values = new Cells( "defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("response_time", new Long(0)),
						CassandraCell.create("response_code", new Integer(200)),
						CassandraCell.create("download_time", new Date()));

		String sql = createTableQueryGenerator(keys, values, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);

		assertEquals(sql,
						"CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
										" (\"id1\" text, \"id2\" timeuuid, \"id3\" int, \"domain_name\" text, \"url\" text, " +
										"\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
										"PRIMARY KEY ((\"id1\", \"id2\"), \"id3\"));"
		);


	}

	@Test
	public void testCreateTableQueryGeneratorSimple() {

		UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");

		Cells keys = new Cells("defaultTable",CassandraCell.create("id1", testTimeUUID, true, false));

		Cells values = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("response_time", new Long(0)),
						CassandraCell.create("response_code", new Integer(200)),
						CassandraCell.create("download_time", new Date()));

		String sql = createTableQueryGenerator(keys, values, OUTPUT_KEYSPACE_NAME, OUTPUT_COLUMN_FAMILY);

		assertEquals(sql,
						"CREATE TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_COLUMN_FAMILY +
										" (\"id1\" timeuuid, \"domain_name\" text, \"url\" text, " +
										"\"response_time\" bigint, \"response_code\" int, \"download_time\" timestamp, " +
										"PRIMARY KEY (\"id1\"));"
		);
	}

	@Test
	public void testPrepareTuple4CqlDriver() {
		UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");
		Date testDate = new Date();

		Cells keys = new Cells("defaultTable",CassandraCell.create("id1", "", true, false),
						CassandraCell.create("id2", testTimeUUID, true, false),
						CassandraCell.create("id3", new Integer(0), false, true));

		Cells values = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("response_time", new Long(0)),
						CassandraCell.create("response_code", new Integer(200)),
						CassandraCell.create("download_time", testDate));

		Tuple2<String[], Object[]>
						bindVars = prepareTuple4CqlDriver(new Tuple2<Cells, Cells>(keys, values));

		String[] names = bindVars._1();
		Object[] vals = bindVars._2();

		assertEquals(names[0], "\"id1\"");
		assertEquals(names[1], "\"id2\"");
		assertEquals(names[2], "\"id3\"");
		assertEquals(names[3], "\"domain_name\"");
		assertEquals(names[4], "\"url\"");
		assertEquals(names[5], "\"response_time\"");
		assertEquals(names[6], "\"response_code\"");
		assertEquals(names[7], "\"download_time\"");

		assertEquals(vals[0], "");
		assertEquals(vals[1], testTimeUUID);
		assertEquals(vals[2], 0);
		assertEquals(vals[3], "");
		assertEquals(vals[4], "");
		assertEquals(vals[5], 0L);
		assertEquals(vals[6], 200);
		assertEquals(vals[7], testDate);
	}
}
