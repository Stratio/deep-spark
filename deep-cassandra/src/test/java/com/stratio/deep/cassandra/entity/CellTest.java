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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.DataType;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepInstantiationException;
import com.stratio.deep.testentity.CommonsTestEntity;
import org.apache.cassandra.db.marshal.*;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Created by luca on 04/02/14.
 */
@Test
public class CellTest {
    private Logger logger = Logger.getLogger(getClass());

    @Test
    public void testCellInstantiationForCollections() throws UnknownHostException, NoSuchFieldException {
        CommonsTestEntity te = new CommonsTestEntity();

        Set<String> emails = new HashSet<>(Arrays.asList("DelfinaMarino@superrito.com", "GabyCasasVeliz@superrito.com"));
        List<String> phones = Arrays.asList("401-477-8301", "209-845-8841");

        te.setEmails(emails);
        te.setPhones(phones);
        Map<UUID, Integer> map = new HashMap<>();
        map.put(UUID.fromString("0E175996-1D5C-4CD1-A0C2-8F4EFAF59F37"), 3211);
        map.put(UUID.fromString("CFC4B1ED-A188-4541-A25B-77098D89A555"), 3212);
        map.put(UUID.fromString("A0C6954F-E576-44C8-94B3-89C9A52BBC7E"), 3213);
        te.setUuid2id(map);

        CassandraCell c1 = (CassandraCell) CassandraCell.create(te, CommonsTestEntity.class.getDeclaredField("emails"));

        assertNotNull(c1);
        assertEquals(c1.getCellName(), "emails");
        assertEquals(SetType.getInstance(UTF8Type.instance).compose(c1.getDecomposedCellValue()), emails);
        assertTrue(c1.marshallerClassName().equals(SetType.class.getCanonicalName()));
        assertEquals(c1.getValueType(), Set.class);

        CassandraCell c2 = (CassandraCell) CassandraCell.create(te, CommonsTestEntity.class.getDeclaredField("phones"));

        assertNotNull(c2);
        assertEquals(c2.getCellName(), "phones");
        assertEquals(ListType.getInstance(UTF8Type.instance).compose(c2.getDecomposedCellValue()), phones);
        assertTrue(c2.marshallerClassName().equals(ListType.class.getCanonicalName()));
        assertEquals(c2.getValueType(), List.class);

        CassandraCell c3 = (CassandraCell) CassandraCell.create(te, CommonsTestEntity.class.getDeclaredField("uuid2id"));

        assertNotNull(c3);
        assertEquals(c3.getCellName(), "uuid2id");
        assertEquals(MapType.getInstance(UUIDType.instance, Int32Type.instance).compose(c3.getDecomposedCellValue()),
                map);
        assertTrue(c3.marshallerClassName().equals(MapType.class.getCanonicalName()));
        assertEquals(c3.getValueType(), Map.class);
    }

    @Test
    public void testCellInstantiation() throws UnknownHostException {
        CassandraCell c1 = (CassandraCell) CassandraCell.create("name", "Luca");

        assertNotNull(c1);
        assertEquals(c1.getCellName(), "name");
        assertEquals(UTF8Type.instance.compose(c1.getDecomposedCellValue()), "Luca");
        assertTrue(c1.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));
        assertEquals(c1.getValueType(), String.class);

        CassandraCell c2 = (CassandraCell) CassandraCell.create("percent", -1.0f);

        assertNotNull(c2);
        assertEquals(c2.getCellName(), "percent");
        assertEquals(FloatType.instance.compose(c2.getDecomposedCellValue()), new Float(-1.0));
        assertTrue(c2.marshallerClassName().equals(FloatType.class.getCanonicalName()));
        assertEquals(c2.getValueType(), Float.class);

        CassandraCell c3 = (CassandraCell) CassandraCell.create("percent", 4);

        assertNotNull(c3);
        assertEquals(c3.getCellName(), "percent");
        assertEquals(Int32Type.instance.compose(c3.getDecomposedCellValue()), new Integer(4));
        assertTrue(c3.marshallerClassName().equals(Int32Type.class.getCanonicalName()));
        assertEquals(c3.getValueType(), Integer.class);

        Date testDate = new Date();
        CassandraCell c4 = (CassandraCell) CassandraCell.create("date", testDate);

        assertNotNull(c4);
        assertEquals(c4.getCellName(), "date");
        assertEquals(TimestampType.instance.compose(c4.getDecomposedCellValue()), testDate);
        assertTrue(c4.marshallerClassName().equals(TimestampType.class.getCanonicalName()));
        assertEquals(c4.getValueType(), Date.class);

        Long testLong = System.currentTimeMillis();
        CassandraCell c6 = (CassandraCell) CassandraCell.create("timeMillis", testLong);

        assertNotNull(c6);
        assertEquals(c6.getCellName(), "timeMillis");
        assertEquals(LongType.instance.compose(c6.getDecomposedCellValue()), testLong);
        assertTrue(c6.marshallerClassName().equals(LongType.class.getCanonicalName()));
        assertEquals(c6.getValueType(), Long.class);

        CassandraCell c7 = (CassandraCell) CassandraCell.create("booltype", Boolean.TRUE);

        assertNotNull(c7);
        assertEquals(c7.getCellName(), "booltype");
        assertTrue(c7.marshallerClassName().equals(BooleanType.class.getCanonicalName()));
        assertEquals(BooleanType.instance.compose(c7.getDecomposedCellValue()), Boolean.TRUE);
        assertEquals(c7.getValueType(), Boolean.class);

        CassandraCell c8 = (CassandraCell) CassandraCell.create("BigDecimalType", new BigDecimal(testLong));

        assertNotNull(c8);
        assertEquals(c8.getCellName(), "BigDecimalType");
        assertEquals(DecimalType.instance.compose(c8.getDecomposedCellValue()), new BigDecimal(testLong));
        assertTrue(c8.marshallerClassName().equals(DecimalType.class.getCanonicalName()));
        assertEquals(c8.getValueType(), BigDecimal.class);

        CassandraCell c9 = (CassandraCell) CassandraCell.create("Doubletype", 100.09);

        assertNotNull(c9);
        assertEquals(c9.getCellName(), "Doubletype");
        assertEquals(DoubleType.instance.compose(c9.getDecomposedCellValue()), 100.09);
        assertTrue(c9.marshallerClassName().equals(DoubleType.class.getCanonicalName()));
        assertEquals(c9.getValueType(), Double.class);

        InetAddress testInet = InetAddress.getLocalHost();
        CassandraCell c10 = (CassandraCell) CassandraCell.create("InetAddressType", testInet);

        assertNotNull(c10);
        assertEquals(c10.getCellName(), "InetAddressType");
        assertEquals(InetAddressType.instance.compose(c10.getDecomposedCellValue()), testInet);
        assertTrue(c10.marshallerClassName().equals(InetAddressType.class.getCanonicalName()));
        assertEquals(c10.getValueType(), InetAddress.class);

        BigInteger testBigInt = new BigInteger("9032809489230884980323498324376012647321674142290");
        CassandraCell c11 = (CassandraCell) CassandraCell.create("BigIntegerType", testBigInt);

        assertNotNull(c11);
        assertEquals(c11.getCellName(), "BigIntegerType");
        assertEquals(IntegerType.instance.compose(c11.getDecomposedCellValue()), testBigInt);
        assertTrue(c11.marshallerClassName().equals(IntegerType.class.getCanonicalName()));
        assertEquals(c11.getValueType(), BigInteger.class);

        UUID testUUID = UUID.randomUUID();
        CassandraCell c12 = (CassandraCell) CassandraCell.create("UUIDType", testUUID);

        assertNotNull(c12);
        assertEquals(c12.getCellName(), "UUIDType");
        assertEquals(UUIDType.instance.compose(c12.getDecomposedCellValue()), testUUID);
        assertTrue(c12.marshallerClassName().equals(UUIDType.class.getCanonicalName()));
        assertEquals(c12.getValueType(), UUID.class);

        UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");
        assertEquals(testTimeUUID.version(), 1);
        CassandraCell c13 = (CassandraCell) CassandraCell.create("TimeUUIDType", testTimeUUID);

        assertNotNull(c13);
        assertEquals(c13.getCellName(), "TimeUUIDType");
        assertEquals(TimeUUIDType.instance.compose(c13.getDecomposedCellValue()), testTimeUUID);
        assertTrue(c13.marshallerClassName().equals(TimeUUIDType.class.getCanonicalName()));
        assertEquals(c13.getValueType(), UUID.class);

        CassandraCell c14 = (CassandraCell) CassandraCell.create(c13, c13.getDecomposedCellValue());
        assertNotNull(c14);
        assertEquals(c14.getCellName(), "TimeUUIDType");
        assertEquals(TimeUUIDType.instance.compose(c13.getDecomposedCellValue()), testTimeUUID);
        assertEquals(c14.getCellValue(), testTimeUUID);
        assertTrue(c14.marshallerClassName().equals(TimeUUIDType.class.getCanonicalName()));
        assertEquals(c14.getValueType(), UUID.class);

        try {
            Cell c15 = (CassandraCell) CassandraCell.create(c13, new CellsTest());
            fail();
        } catch (DeepInstantiationException d) {
            logger.info("correctly catched DeepInstantiationException");
        }

        try {
            Cell c15 = (CassandraCell) CassandraCell.create("my cell name", new CellsTest());
            fail();
        } catch (DeepInstantiationException d) {
            logger.info("correctly catched DeepInstantiationException");
        }

        try {
            Cell c15 = (CassandraCell) CassandraCell.create("my cell name", new CellsTest(), true, true);
            fail();
        } catch (DeepInstantiationException d) {
            logger.info("correctly catched DeepInstantiationException");
        }
    }

    @Test
    public void testCellWithNullValues() {
        Long testLong = System.currentTimeMillis();
        CassandraCell c8 = (CassandraCell) CassandraCell.create("BigDecimalType", new BigDecimal(testLong));
        CassandraCell c16 = (CassandraCell) CassandraCell.create("no_value_cell");

        assertNotNull(c16);
        assertEquals(c16.getCellName(), "no_value_cell");
        assertNull(c16.getCellValue());
        assertFalse(c16.equals(c8));
        assertFalse(c8.equals(c16));
        assertNull(c16.getCellValidator());
        assertEquals(c16.getDecomposedCellValue(), ByteBuffer.wrap(new byte[0]));
        assertFalse(c16.isPartitionKey());
        assertFalse(c16.isClusterKey());
        assertNull(c16.marshallerClassName());
        assertNull(c16.marshaller());
        assertTrue(c16.hashCode() != 0);
    }

    @Test
    public void testCellInstantiationWithByteBuffer() {
        ByteBuffer bb = UTF8Type.instance.decompose("Test string");

        CassandraCell metadata = (CassandraCell) CassandraCell.create("id", DataType.text(), false, true);

        CassandraCell c = (CassandraCell) CassandraCell.create(metadata, bb);

        assertEquals(c.getDecomposedCellValue(), UTF8Type.instance.decompose("Test string"));
        assertTrue(c.isClusterKey());
        assertFalse(c.isPartitionKey());
        assertEquals(c.getCellName(), "id");
        assertEquals("Test string", c.getCellValue());
        assertTrue(c.hashCode() != 0);

        CassandraCell nullCell = (CassandraCell) CassandraCell.create("nullCell", DataType.text(), false, true);
        assertNull(nullCell.getCellValue());
        assertEquals(nullCell.getDecomposedCellValue(), ByteBuffer.wrap(new byte[0]));
        assertTrue(nullCell.hashCode() != 0);
    }

    @Test
    public void testEquality() {

        UUID id = UUID.randomUUID();
        Cell c = (CassandraCell) CassandraCell.create("id", id, true, false);

        assertFalse(c.equals(new Integer(1)));
        assertTrue(c.equals(c));
        assertFalse(c.equals((CassandraCell) CassandraCell.create("id", id, false, false)));
        assertFalse(c.equals((CassandraCell) CassandraCell.create("id", id, true, true)));
        assertTrue(c.equals((CassandraCell) CassandraCell.create("id", id, true, false)));
    }

    @Test
    public void testNullCellEquality() {

        Cell nullCell1 = (CassandraCell) CassandraCell.create("nullCell1");
        Cell nullCell2 = (CassandraCell) CassandraCell.create("nullCell1");

        assertTrue(nullCell1.equals(nullCell2));
    }

    @Test
    public void testWrongCellInstantiation() {
        try {
            CassandraCell.create("name", new DeepGenericException());

            fail();
        } catch (DeepGenericException e) {
            // ok
            logger.info("Correctly catched excepcion: " + e);
        }

        try {
            UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");
            Cell c13 = (CassandraCell) CassandraCell.create("TimeUUIDType", testTimeUUID);

            CassandraCell.create(c13, Int32Type.instance.decompose(456));

            fail();
        } catch (Exception e) {
            // ok
            logger.info("Correctly catched excepcion: " + e);
        }
    }

}
