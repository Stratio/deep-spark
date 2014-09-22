/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.cassandra.entity;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepGenericException;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test
public class CellsTest {

    @Test
    public void testInstantiation() {
        Cells cells = new Cells("defaultTable");
        assertEquals(cells.size(), 0);

        Cells keys = new Cells("defaultTable",CassandraCell.create("id1", "", true, false), CassandraCell.create("id2", "", false, true));

        assertEquals(keys.size(), 2);

        Cells values = new Cells("defaultTable",CassandraCell.create("domain_name", ""), CassandraCell.create("url", ""), CassandraCell.create("response_time",
                ""), CassandraCell.create("response_code", ""), CassandraCell.create("download_time", ""));

        assertEquals(values.size(), 5);

    }

	@Test
	public void testInstantiationForTable() {
		try {
			new Cells((String)null, CassandraCell.create("domain_name", ""));

			fail();
		} catch (IllegalArgumentException dge) {
			// ok
		} catch (Exception e) {
			fail();
		}

		try {
			new Cells("", CassandraCell.create("domain_name", ""));

			fail();
		} catch (IllegalArgumentException dge) {
			// ok
		} catch (Exception e) {
			fail();
		}

		Cells cells = new Cells("myTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("page_name", ""));

		assertEquals(cells.size(), 2);
	}

    @Test
    public void testAdd() {

        Cells cells = new Cells("defaultTable");

        try {
            cells.add(null);

            fail();
        } catch (DeepGenericException dge) {
            // ok
        } catch (Exception e) {
            fail();
        }

        cells.add(CassandraCell.create("domain_name", ""));
        assertEquals(cells.size(), 1);
    }

	@Test
	public void testAddForTables() {
		Cells cells = new Cells("defaultTable");
		assertTrue(cells.isEmpty());

		try {
			cells.add(null, CassandraCell.create("domain_name", ""));

			fail();
		} catch (IllegalArgumentException dge) {
			// ok
		} catch (Exception e) {
			fail();
		}

		try {
			cells.add("", CassandraCell.create("domain_name", ""));

			fail();
		} catch (IllegalArgumentException dge) {
			// ok
		} catch (Exception e) {
			fail();
		}

		cells.add("myFirstTable", CassandraCell.create("domain_name", ""));
		assertFalse(cells.isEmpty());
		cells.add("myFirstTable", CassandraCell.create("id1", "payload1", true, false));
		cells.add("myFirstTable", CassandraCell.create("id2", "payload1", false, true));

		cells.add("mySecondTable", CassandraCell.create("id1", "payload1", true, false));
		assertFalse(cells.isEmpty());
		cells.add("mySecondTable", CassandraCell.create("id2", "payload1", false, true));

		assertEquals(cells.size(), 5);
		assertFalse(cells.isEmpty());

		assertEquals(cells.size("myFirstTable"),3);
		assertEquals(cells.size("mySecondTable"),2);
		assertEquals(cells.size("defaultTable"),0);
	}

    @Test
    public void testGetCellByIdx() {
        Cells keys = new Cells("defaultTable",CassandraCell.create("id1", "payload1", true, false), CassandraCell.create("id2", "payload2", false,
                true));

        try {
            keys.getCellByIdx(-1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // ok
        } catch (Exception e) {
            fail();
        }

        try {
            keys.getCellByIdx(4);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // ok
        } catch (Exception e) {
            fail();
        }

        CassandraCell c = (CassandraCell) keys.getCellByIdx(1);

        assertNotNull(c);
        assertEquals(c.getCellName(), "id2");
        assertEquals(UTF8Type.instance.compose(c.getDecomposedCellValue()), "payload2");
        assertTrue(c.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));
    }

	@Test
	public void testGetCellByIdxForTables() {
		Cells keys = new Cells("myFirstTable",
						CassandraCell.create("id1", "payload1", true, false),
						CassandraCell.create("id2", "payload2", false, true));

		keys.add("mySecondTable", CassandraCell.create("id2", "payload2", false, true));

		try {
			keys.getCellByIdx(-1);
			fail();
		} catch (IndexOutOfBoundsException e) {
			// ok
		} catch (Exception e) {
			fail();
		}

		try {
			keys.getCellByIdx(4);
			fail();
		} catch (IndexOutOfBoundsException e) {
			// ok
		} catch (Exception e) {
			fail();
		}

		try {
			keys.getCellByIdx("myFirstTable",-1);
			fail();
		} catch (IndexOutOfBoundsException e) {
			// ok
		} catch (Exception e) {
			fail();
		}

		try {
			keys.getCellByIdx("mySecondTable",-1);
			fail();
		} catch (IndexOutOfBoundsException e) {
			// ok
		} catch (Exception e) {
			fail();
		}

        CassandraCell c = (CassandraCell) keys.getCellByIdx("myFirstTable",1);

		assertNotNull(c);
		assertEquals(c.getCellName(), "id2");
		assertEquals(UTF8Type.instance.compose(c.getDecomposedCellValue()), "payload2");
		assertTrue((c.marshallerClassName().equals(UTF8Type.class.getCanonicalName())));

		c = (CassandraCell) keys.getCellByIdx("mySecondTable",0);

		assertNotNull(c);
		assertEquals(c.getCellName(), "id2");
		assertEquals(UTF8Type.instance.compose(c.getDecomposedCellValue()), "payload2");
		assertTrue(c.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));
	}

    @Test
    public void testGetCellByName() {
        Cells values = new Cells("defaultTable",CassandraCell.create("domain_name", "abc.es"), CassandraCell.create("url", ""), CassandraCell.create(
                "response_time", ""), CassandraCell.create("response_code", ""), CassandraCell.create("download_time", ""));

        assertNull(values.getCellByName("notexistingcell"));
        CassandraCell c = (CassandraCell) values.getCellByName("domain_name");
        assertNotNull(c);
        assertEquals(c.getCellName(), "domain_name");
        assertEquals(UTF8Type.instance.compose(c.getDecomposedCellValue()), "abc.es");
        assertTrue(c.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));
    }

    @Test
    public void testGetCells() {
        Cells values = new Cells("defaultTable",CassandraCell.create("domain_name", "abc.es"), CassandraCell.create("url", ""), CassandraCell.create(
                "response_time", ""), CassandraCell.create("response_code", ""), CassandraCell.create("download_time", ""));

        Collection<Cell> copy = values.getCells();

        assertNotNull(copy);
        assertEquals(copy.size(), 5);
    }

	@Test
	public void testGetCellsForTable() {
		Cells values = new Cells("firstTable",
						CassandraCell.create("domain_name", "abc.es"),
						CassandraCell.create("url", ""),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("response_code", ""),
						CassandraCell.create("download_time", ""));

		values.add("secondTable", CassandraCell.create("domain_name", "abc.es"));
		values.add("secondTable", CassandraCell.create("url", ""));
		values.add("secondTable", CassandraCell.create("response_time", ""));
		values.add("secondTable", CassandraCell.create("download_time", ""));

		values.add(CassandraCell.create("default_table_columns", "abc"));

		Collection<Cell> copy = values.getCells();

		assertNotNull(copy);
		assertEquals(copy.size(), 10);

		copy = values.getCells("firstTable");

		assertNotNull(copy);
		assertEquals(copy.size(), 6);

		copy = values.getCells("secondTable");

		assertNotNull(copy);
		assertEquals(copy.size(), 4);

		copy = values.getCells("noTable");

		assertNotNull(copy);
		assertEquals(copy.size(), 0);
	}

	@Test
	public void testGetInternalCells(){
		Cells values = new Cells("firstTable",
						CassandraCell.create("domain_name", "abc.es"),
						CassandraCell.create("url", ""),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("response_code", ""),
						CassandraCell.create("download_time", ""));

		values.add("secondTable", CassandraCell.create("domain_name", "abc.es"));
		values.add("secondTable", CassandraCell.create("url", ""));
		values.add("secondTable", CassandraCell.create("response_time", ""));
		values.add("secondTable", CassandraCell.create("download_time", ""));

		values.add(CassandraCell.create("default_table_columns", "abc"));

		Map<String, List<Cell>> internalRepresentation =
						values.getInternalCells();

		assertNotNull(internalRepresentation);
		assertEquals(internalRepresentation.size(), 2);
		assertEquals(internalRepresentation.get("firstTable").size(), 6);
		assertEquals(internalRepresentation.get("secondTable").size(), 4);
		assertNull(internalRepresentation.get("noTable"));
	}

    @Test
    public void testGetDecomposedCellValues() {
        long downloadTime = System.currentTimeMillis();
        Cells values = new Cells("defaultTable",CassandraCell.create("domain_name", "abc.es"), CassandraCell.create("url", "http://www.abc.es"),
                CassandraCell.create("response_time", 102), CassandraCell.create("response_code", 200), CassandraCell.create("download_time",
                downloadTime)
        );

        List<ByteBuffer> dcv = (List<ByteBuffer>) values.getDecomposedCellValues();
        ByteBuffer bb0 = dcv.get(0);
        assertNotNull(bb0);
        assertEquals(bb0, UTF8Type.instance.decompose("abc.es"));

        ByteBuffer bb1 = dcv.get(1);
        assertEquals(bb1, UTF8Type.instance.decompose("http://www.abc.es"));

        ByteBuffer bb2 = dcv.get(2);
        assertEquals(bb2, Int32Type.instance.decompose(102));

        ByteBuffer bb3 = dcv.get(3);
        assertEquals(bb3, Int32Type.instance.decompose(200));

        ByteBuffer bb4 = dcv.get(4);
        assertEquals(bb4, LongType.instance.decompose(downloadTime));
    }

	@Test
	public void testGetDecomposedCellValuesForTable() {
		long downloadTime = System.currentTimeMillis();
		Cells values = new Cells("firstTable",
						CassandraCell.create("domain_name", "abc.es"),
						CassandraCell.create("url", "http://www.abc.es"),
						CassandraCell.create("response_time", 102),
						CassandraCell.create("response_code", 200),
						CassandraCell.create("download_time", downloadTime));

		values.add("secondTable", CassandraCell.create("domain_name", "abc.es"));
		values.add("secondTable", CassandraCell.create("url", ""));
		values.add("secondTable", CassandraCell.create("response_time", 432));
		values.add("secondTable", CassandraCell.create("download_time", 3829432));


		List<ByteBuffer> dcv = (List<ByteBuffer>) values.getDecomposedCellValues("firstTable");
		assertEquals(dcv.size(), 5);
		ByteBuffer bb0 = dcv.get(0);
		assertNotNull(bb0);
		assertEquals(bb0, UTF8Type.instance.decompose("abc.es"));

		ByteBuffer bb1 = dcv.get(1);
		assertEquals(bb1, UTF8Type.instance.decompose("http://www.abc.es"));

		ByteBuffer bb2 = dcv.get(2);
		assertEquals(bb2, Int32Type.instance.decompose(102));

		ByteBuffer bb3 = dcv.get(3);
		assertEquals(bb3, Int32Type.instance.decompose(200));

		ByteBuffer bb4 = dcv.get(4);
		assertEquals(bb4, LongType.instance.decompose(downloadTime));

		dcv = (List<ByteBuffer>) values.getDecomposedCellValues("secondTable");

		bb0 = dcv.get(0);
		assertNotNull(bb0);
		assertEquals(bb0, UTF8Type.instance.decompose("abc.es"));

		bb1 = dcv.get(1);
		assertEquals(bb1, UTF8Type.instance.decompose(""));

		bb2 = dcv.get(2);
		assertEquals(bb2, Int32Type.instance.decompose(432));

		bb3 = dcv.get(3);
		assertEquals(bb3, Int32Type.instance.decompose(3829432));
	}

    @Test
    public void testIterability() {
        long downloadTime = System.currentTimeMillis();

        Cells values = new Cells("defaultTable",CassandraCell.create("domain_name", "abc.es"), CassandraCell.create("url", "http://www.abc.es"),
                CassandraCell.create("response_time", 102), CassandraCell.create("response_code", 200), CassandraCell.create("download_time",
                downloadTime)
        );

        int idx = 0;
        for (Cell cell : values) {
            ByteBuffer bb = ((CassandraCell)cell).getDecomposedCellValue();

            switch (idx) {
                case 0:
                    assertEquals(bb, UTF8Type.instance.decompose("abc.es"));
                    break;
                case 1:
                    assertEquals(bb, UTF8Type.instance.decompose("http://www.abc.es"));
                    break;
                case 2:
                    assertEquals(bb, Int32Type.instance.decompose(102));
                    break;
                case 3:
                    assertEquals(bb, Int32Type.instance.decompose(200));
                    break;
                case 4:
                    assertEquals(bb, LongType.instance.decompose(downloadTime));
                    break;
                default:
                    fail();
                    break;
            }

            ++idx;
        }
    }

    @Test
    public void testSplitCells() {

        Cells cells = new Cells("defaultTable",
				        CassandraCell.create("domain_name", ""),
				        CassandraCell.create("id2", "", false, true),
				        CassandraCell.create("response_time", ""),
				        CassandraCell.create("url", ""),
				        CassandraCell.create("id1", "", true, false),
				        CassandraCell.create("response_code", ""),
				        CassandraCell.create("download_time", ""));

        Cells keys = cells.getIndexCells();
        assertNotNull(keys);
        assertTrue(keys.equals(new Cells("defaultTable",CassandraCell.create("id2", "", false, true), CassandraCell.create("id1", "", true, false))));

        Cells values = cells.getValueCells();
        assertNotNull(values);
        assertTrue(values.equals(
                new Cells("defaultTable",CassandraCell.create("domain_name", ""), CassandraCell.create("response_time", ""), CassandraCell.create("url", ""),
                        CassandraCell.create("response_code", ""), CassandraCell.create("download_time", ""))
        ));
    }

	@Test
	public void testSplitCellsForTable() {

		Cells cells = new Cells("firstTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""));

		cells.add("secondTable",CassandraCell.create("url", ""));
		cells.add("secondTable",CassandraCell.create("id1", "", true, false));
		cells.add("secondTable",CassandraCell.create("response_code","", false, true));
		cells.add("secondTable",CassandraCell.create("download_time", ""));

		Cells keys = cells.getIndexCells("firstTable");
		assertNotNull(keys);
		assertEquals(keys.getCells("firstTable").size(), 1);

		keys = cells.getIndexCells("secondTable");
		assertNotNull(keys);
		assertEquals(keys.getCells("secondTable").size(), 2);


		Cells values = cells.getValueCells("firstTable");
		assertNotNull(values);
		assertEquals(values.getCells("firstTable").size(), 2);

		values = cells.getValueCells("secondTable");
		assertNotNull(values);
		assertEquals(values.getCells("secondTable").size(), 2);

		keys = cells.getIndexCells();
		assertEquals(keys.getInternalCells().size(),2);
		assertEquals(keys.getCells("firstTable").size(), 1);
		assertEquals(keys.getCells("secondTable").size(), 2);
		assertEquals(keys.getCells().size(), 3);

		values = cells.getValueCells();
		assertEquals(values.getInternalCells().size(),2);
		assertEquals(values.getCells("firstTable").size(),2);
		assertEquals(values.getCells("secondTable").size(), 2);
		assertEquals(values.getCells().size(), 4);
	}

	@Test
	public void testEquals() {

		Cells cells = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("id1", "", true, false),
						CassandraCell.create("response_code", ""));

		Cells cells2 = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("id1", "", true, false),
						CassandraCell.create("response_code", ""));

		assertTrue(cells.equals(cells2));
		assertTrue(cells2.equals(cells));

	}

	@Test
    public void testNotEquals() {
        Cells cells = new Cells("defaultTable",
				        CassandraCell.create("domain_name", ""),
				        CassandraCell.create("id2", "", false, true),
				        CassandraCell.create("response_time", ""),
				        CassandraCell.create("url", ""),
				        CassandraCell.create("id1", "", true, false),
				        CassandraCell.create("response_code", ""),
				        CassandraCell.create("download_time", ""));

		assertFalse(cells.equals(new Integer(1)));

		Cells cells2 = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("id1", "", true, false),
						CassandraCell.create("response_code", ""));

		assertFalse(cells.equals(cells2));

		cells2.add("secondTable",CassandraCell.create("download_time", ""));
		assertFalse(cells.equals(cells2));
		assertFalse(cells2.equals(cells));

		cells2 = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("id1", "", true, false),
						CassandraCell.create("response_code", ""),
						CassandraCell.create("download_time",342));

		assertFalse(cells.equals(cells2));
		assertFalse(cells2.equals(cells));

		Cells values1 = new Cells("firstTable",
						CassandraCell.create("domain_name", "abc.es"));

		Cells values2 = new Cells("secondTable",
						CassandraCell.create("domain_name", "abc.es"));
		assertFalse(values1.equals(values2));
		assertFalse(values2.equals(values1));

    }

	@Test
	public void testRemove(){

		Cells cells = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("id1", "", true, false),
						CassandraCell.create("response_code", ""),
						CassandraCell.create("download_time", ""));

		try {
			cells.remove(null);
			fail();
		} catch (DeepGenericException e){
			// ok
		}

		assertTrue(cells.remove("url"));
		assertEquals(cells.size(), 6);

		assertFalse(cells.remove("url"));
		assertEquals(cells.size(), 6);

		cells.add("secondTable", CassandraCell.create("domain_name", "abc.es"));
		cells.add("secondTable", CassandraCell.create("url", ""));
		cells.add("secondTable", CassandraCell.create("response_time", 432));
		cells.add("secondTable", CassandraCell.create("download_time", 3829432));

		assertTrue(cells.remove("id1"));
		assertEquals(cells.size(), 9);

		assertEquals(cells.size("secondTable"), 4);

		assertFalse(cells.remove("secondTable", "not_existent"));
		assertEquals(cells.size("secondTable"), 4);

		assertTrue(cells.remove("secondTable", "response_time"));
		assertEquals(cells.size("secondTable"), 3);

		assertEquals(cells.size(), 8);
	}

	@Test
	public void replaceByName(){
		Cells cells = new Cells("defaultTable",
						CassandraCell.create("domain_name", ""),
						CassandraCell.create("id2", "", false, true),
						CassandraCell.create("response_time", ""),
						CassandraCell.create("url", ""),
						CassandraCell.create("id1", "", true, false),
						CassandraCell.create("response_code", ""),
						CassandraCell.create("download_time", ""));
		try {
			cells.remove(null);
			fail();
		} catch (DeepGenericException e){
			// ok
		}

		assertTrue(cells.replaceByName(CassandraCell.create("download_time", 48372)));
		assertFalse(cells.replaceByName(CassandraCell.create("no_existent", 48372)));

		cells.add("secondTable", CassandraCell.create("domain_name", "abc.es"));
		cells.add("secondTable", CassandraCell.create("url", ""));
		cells.add("secondTable", CassandraCell.create("response_time", 432));
		cells.add("secondTable", CassandraCell.create("download_time", 3829432));

		assertTrue(cells.replaceByName("secondTable", CassandraCell.create("url", "http://abc")));
		assertFalse(cells.replaceByName("secondTable", CassandraCell.create("response_code", 48372)));
		assertFalse(cells.replaceByName("secondTable", CassandraCell.create("no_existent", 48372)));
	}
}
