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

package com.stratio.deep.testentity;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import static org.testng.Assert.*;

@Test
public class CellsTest {

    @Test
    public void testInstantiation() {
        Cells cells = new Cells();
        assertEquals(cells.size(), 0);

        Cells keys = new Cells(Cell.create("id1", "", true, false), Cell.create("id2", "", false, true));

        assertEquals(keys.size(), 2);

        Cells values = new Cells(Cell.create("domain_name", ""), Cell.create("url", ""), Cell.create("response_time",
                ""), Cell.create("response_code", ""), Cell.create("download_time", ""));

        assertEquals(values.size(), 5);

    }

    @Test
    public void testAdd() {

        Cells cells = new Cells();

        try {
            cells.add(null);

            fail();
        } catch (DeepGenericException dge) {
            // ok
        } catch (Exception e) {
            fail();
        }

        cells.add(Cell.create("domain_name", ""));
        assertEquals(cells.size(), 1);
    }

    @Test
    public void testGetCellByIdx() {
        Cells keys = new Cells(Cell.create("id1", "payload1", true, false), Cell.create("id2", "payload2", false,
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

        Cell c = keys.getCellByIdx(1);

        assertNotNull(c);
        assertEquals(c.getCellName(), "id2");
        assertEquals(UTF8Type.instance.compose(c.getDecomposedCellValue()), "payload2");
        assertTrue(c.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));
    }

    @Test
    public void testGetCellByName() {
        Cells values = new Cells(Cell.create("domain_name", "abc.es"), Cell.create("url", ""), Cell.create(
                "response_time", ""), Cell.create("response_code", ""), Cell.create("download_time", ""));

        assertNull(values.getCellByName("notexistingcell"));
        Cell c = values.getCellByName("domain_name");
        assertNotNull(c);
        assertEquals(c.getCellName(), "domain_name");
        assertEquals(UTF8Type.instance.compose(c.getDecomposedCellValue()), "abc.es");
        assertTrue(c.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));
    }

    @Test
    public void testGetCells() {
        Cells values = new Cells(Cell.create("domain_name", "abc.es"), Cell.create("url", ""), Cell.create(
                "response_time", ""), Cell.create("response_code", ""), Cell.create("download_time", ""));

        Collection<Cell> copy = values.getCells();

        assertNotNull(copy);
        assertEquals(copy.size(), 5);
    }

    @Test
    public void testGetDecomposedCellValues() {
        long downloadTime = System.currentTimeMillis();
        Cells values = new Cells(Cell.create("domain_name", "abc.es"), Cell.create("url", "http://www.abc.es"),
                Cell.create("response_time", 102), Cell.create("response_code", 200), Cell.create("download_time",
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
    public void testIterability() {
        long downloadTime = System.currentTimeMillis();

        Cells values = new Cells(Cell.create("domain_name", "abc.es"), Cell.create("url", "http://www.abc.es"),
                Cell.create("response_time", 102), Cell.create("response_code", 200), Cell.create("download_time",
                downloadTime)
        );

        int idx = 0;
        for (Cell cell : values) {
            ByteBuffer bb = cell.getDecomposedCellValue();

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

        Cells cells = new Cells(Cell.create("domain_name", ""), Cell.create("id2", "", false, true), Cell.create(
                "response_time", ""), Cell.create("url", ""), Cell.create("id1", "", true, false), Cell.create(
                "response_code", ""), Cell.create("download_time", ""));

        Cells keys = cells.getIndexCells();
        assertNotNull(keys);
        assertTrue(keys.equals(new Cells(Cell.create("id2", "", false, true), Cell.create("id1", "", true, false))));

        Cells values = cells.getValueCells();
        assertNotNull(values);
        assertTrue(values.equals(
                new Cells(Cell.create("domain_name", ""), Cell.create("response_time", ""), Cell.create("url", ""),
                        Cell.create("response_code", ""), Cell.create("download_time", ""))
        ));
    }

    public void testNotEquals() {
        Cells cells = new Cells(Cell.create("domain_name", ""), Cell.create("id2", "", false, true), Cell.create(
                "response_time", ""), Cell.create("url", ""), Cell.create("id1", "", true, false), Cell.create(
                "response_code", ""), Cell.create("download_time", ""));

        assertFalse(cells.equals(new Integer(1)));

        Cells keys = new Cells(Cell.create("id1", "payload1", true, false), Cell.create("id2", "payload2", false, true));

        assertFalse(cells.equals(keys));
    }


}
