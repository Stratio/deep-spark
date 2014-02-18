package com.stratio.deep.entity;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.stratio.deep.exception.DeepGenericException;
import org.apache.cassandra.db.marshal.*;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Created by luca on 04/02/14.
 */
@Test
public class CellTest {

    @Test
    public void testCellInstantiation() throws UnknownHostException {
	Cell<?> c1 = Cell.create("name", "Luca");

	assertNotNull(c1);
	assertEquals(c1.getCellName(), "name");
	assertEquals(UTF8Type.instance.compose(c1.getDecomposedCellValue()), "Luca");
	assertTrue(c1.marshallerClassName().equals(UTF8Type.class.getCanonicalName()));

	Cell<?> c2 = Cell.create("percent", -1.0f);

	assertNotNull(c2);
	assertEquals(c2.getCellName(), "percent");
	assertEquals(FloatType.instance.compose(c2.getDecomposedCellValue()), new Float(-1.0));
	assertTrue(c2.marshallerClassName().equals(FloatType.class.getCanonicalName()));

	Cell<?> c3 = Cell.create("percent", 4);

	assertNotNull(c3);
	assertEquals(c3.getCellName(), "percent");
	assertEquals(Int32Type.instance.compose(c3.getDecomposedCellValue()), new Integer(4));
	assertTrue(c3.marshallerClassName().equals(Int32Type.class.getCanonicalName()));

	Date testDate = new Date();
	Cell<?> c4 = Cell.create("date", testDate);

	assertNotNull(c4);
	assertEquals(c4.getCellName(), "date");
	assertEquals(TimestampType.instance.compose(c4.getDecomposedCellValue()), testDate);
	assertTrue(c4.marshallerClassName().equals(TimestampType.class.getCanonicalName()));

	Long testLong = System.currentTimeMillis();
	Cell<?> c6 = Cell.create("timeMillis", testLong);

	assertNotNull(c6);
	assertEquals(c6.getCellName(), "timeMillis");
	assertEquals(LongType.instance.compose(c6.getDecomposedCellValue()), testLong);
	assertTrue(c6.marshallerClassName().equals(LongType.class.getCanonicalName()));

	Cell<?> c7 = Cell.create("booltype", Boolean.TRUE);

	assertNotNull(c7);
	assertEquals(c7.getCellName(), "booltype");
	assertTrue(c7.marshallerClassName().equals(BooleanType.class.getCanonicalName()));
	assertEquals(BooleanType.instance.compose(c7.getDecomposedCellValue()), Boolean.TRUE);

	Cell<?> c8 = Cell.create("BigDecimalType", new BigDecimal(testLong));

	assertNotNull(c8);
	assertEquals(c8.getCellName(), "BigDecimalType");
	assertEquals(DecimalType.instance.compose(c8.getDecomposedCellValue()), new BigDecimal(testLong));
	assertTrue(c8.marshallerClassName().equals(DecimalType.class.getCanonicalName()));

	Cell<?> c9 = Cell.create("Doubletype", new Double(100.09));

	assertNotNull(c9);
	assertEquals(c9.getCellName(), "Doubletype");
	assertEquals(DoubleType.instance.compose(c9.getDecomposedCellValue()), new Double(100.09));
	assertTrue(c9.marshallerClassName().equals(DoubleType.class.getCanonicalName()));

	InetAddress testInet = InetAddress.getLocalHost();
	Cell<?> c10 = Cell.create("InetAddressType", testInet);

	assertNotNull(c10);
	assertEquals(c10.getCellName(), "InetAddressType");
	assertEquals(InetAddressType.instance.compose(c10.getDecomposedCellValue()), testInet);
	assertTrue(c10.marshallerClassName().equals(InetAddressType.class.getCanonicalName()));

	BigInteger testBigInt = new BigInteger("9032809489230884980323498324376012647321674142290");
	Cell<?> c11 = Cell.create("BigIntegerType", testBigInt);

	assertNotNull(c11);
	assertEquals(c11.getCellName(), "BigIntegerType");
	assertEquals(IntegerType.instance.compose(c11.getDecomposedCellValue()), testBigInt);
	assertTrue(c11.marshallerClassName().equals(IntegerType.class.getCanonicalName()));

	UUID testUUID = UUID.randomUUID();
	Cell<?> c12 = Cell.create("UUIDType", testUUID);

	assertNotNull(c12);
	assertEquals(c12.getCellName(), "UUIDType");
	assertEquals(UUIDType.instance.compose(c12.getDecomposedCellValue()), testUUID);
	assertTrue(c12.marshallerClassName().equals(UUIDType.class.getCanonicalName()));

	UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");
	assertEquals(testTimeUUID.version(), 1);
	Cell<?> c13 = Cell.create("TimeUUIDType", testTimeUUID);

	assertNotNull(c13);
	assertEquals(c13.getCellName(), "TimeUUIDType");
	assertEquals(TimeUUIDType.instance.compose(c13.getDecomposedCellValue()), testTimeUUID);
	assertTrue(c13.marshallerClassName().equals(TimeUUIDType.class.getCanonicalName()));

    }

    @Test
    public void testCellInstantiationWithByteBuffer() {
	ByteBuffer bb = UTF8Type.instance.decompose("Test string");
	Cell<?> c = Cell.create("id", bb, UTF8Type.class.getCanonicalName(), false, true);

	assertEquals(c.getDecomposedCellValue(), UTF8Type.instance.decompose("Test string"));
	assertTrue(c.isClusterKey());
	assertFalse(c.isPartitionKey());
	assertEquals(c.getCellName(), "id");
	assertEquals("Test string", c.getCellValue());

	Cell<?> nullCell = Cell.create("nullCell", null);
	assertNull(nullCell.getCellValue());
	assertEquals(nullCell.getDecomposedCellValue(), ByteBuffer.wrap(new byte[0]));

    }

    @Test
    public void testEquality() {

	UUID id = UUID.randomUUID();
	Cell<?> c = Cell.create("id", id, true, false);

	assertFalse(c.equals(new Integer(1)));
	assertTrue(c.equals(c));
	assertFalse(c.equals(Cell.create("id2", null)));
	assertFalse(c.equals(Cell.create("id", null)));
	assertFalse(c.equals(Cell.create("id", id, false, false)));
	assertFalse(c.equals(Cell.create("id", id, true, true)));
	assertTrue(c.equals(Cell.create("id", id, true, false)));

    }

    @Test
    public void testWrongCellInstantiation() {
	try {
	    Cell.create("name", new DeepGenericException());

	    fail();
	} catch (DeepGenericException e) {
	    // ok
	}
    }

}
