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

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.DataType;
import com.stratio.deep.testentity.CommonsTestEntity;
import javax.management.relation.RoleList;
import javax.management.relation.RoleUnresolvedList;
import javax.print.attribute.standard.PrinterStateReasons;
import org.apache.cassandra.db.marshal.*;
import org.testng.annotations.Test;

import static com.stratio.deep.cassandra.entity.CellValidator.Kind;
import static com.stratio.deep.cassandra.entity.CellValidator.cellValidator;
import static org.testng.Assert.*;

/**
 * Created by luca on 25/03/14.
 */
@Test
public class CellValidatorTest {

    public void testEmptyInstantiation() {
        CellValidator cv0 = cellValidator("hello");
        assertNotNull(cv0);
        assertEquals(cv0.getValidatorClassName(), "org.apache.cassandra.db.marshal.UTF8Type");
        assertNull(cv0.getValidatorTypes());
        assertEquals(cv0.validatorKind(), Kind.NOT_A_COLLECTION);

        assertNotNull(cv0.getAbstractType());
        assertEquals(cv0.getAbstractType(), UTF8Type.instance);
    }

    public void testDataTypeNoCollectionInstantiation() {
        DataType type = DataType.inet();

        CellValidator cv = cellValidator(type);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), InetAddressType.class.getCanonicalName());
        assertNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.NOT_A_COLLECTION);
        assertEquals(DataType.Name.INET, cv.getCqlTypeName());

        assertNotNull(cv.getAbstractType());
        assertEquals(cv.getAbstractType(), InetAddressType.instance);
    }

    public void testDataTypeSetInstantiation() {
        DataType type = DataType.set(DataType.text());

        CellValidator cv = cellValidator(type);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), SetType.class.getName());
        assertNotNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.SET);
        assertEquals(cv.getValidatorTypes().size(), 1);
        assertEquals(cv.getValidatorTypes().iterator().next(), "text");
        assertEquals(DataType.Name.SET, cv.getCqlTypeName());

        try {
            Collection<String> types = cv.getValidatorTypes();
            types.add("test");
            fail("Validator types collection must be inmutable");
        } catch (Exception ex) {
            // ok
        }

        assertNotNull(cv.getAbstractType());
        assertEquals(cv.getAbstractType(), SetType.getInstance(UTF8Type.instance));
    }

    public void testDataTypeListInstantiation() {
        try {
            assertNull(cellValidator((DataType) null));
            fail();
        } catch (Exception e) {
            //ok
        }

        DataType type = DataType.list(DataType.timeuuid());

        CellValidator cv = cellValidator(type);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), ListType.class.getName());
        assertNotNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.LIST);
        assertEquals(cv.getValidatorTypes().size(), 1);
        assertEquals(cv.getValidatorTypes().iterator().next(), "timeuuid");
        assertEquals(DataType.Name.LIST, cv.getCqlTypeName());

        try {
            Collection<String> types = cv.getValidatorTypes();
            types.add("test");
            fail("Validator types collection must be inmutable");
        } catch (Exception ex) {
            // ok
        }

        assertNotNull(cv.getAbstractType());
        assertEquals(cv.getAbstractType(), ListType.getInstance(TimeUUIDType.instance));
    }

    public void testBlobDataType(){
        CellValidator cv = cellValidator(DataType.blob());
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), BytesType.class.getName());
        assertNull(cv.getValidatorTypes());
        assertNotNull(cv.getAbstractType());
        assertEquals(cv.getAbstractType(), BytesType.instance);
    }

    public void testDataTypeMapInstantiation() {
        DataType type = DataType.map(DataType.text(), DataType.bigint());

        CellValidator cv = cellValidator(type);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), MapType.class.getName());
        assertNotNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.MAP);
        assertEquals(cv.getValidatorTypes().size(), 2);
        Iterator<String> types = cv.getValidatorTypes().iterator();
        assertEquals(types.next(), "text");
        assertEquals(types.next(), "bigint");
        assertEquals(DataType.Name.MAP, cv.getCqlTypeName());

        try {
            Collection<String> ctypes = cv.getValidatorTypes();
            ctypes.add("test");
            fail("Validator types collection must be inmutable");
        } catch (Exception ex) {
            // ok
        }

        assertNotNull(cv.getAbstractType());
        assertEquals(cv.getAbstractType(), MapType.getInstance(UTF8Type.instance, LongType.instance));
    }

    public void testEquality() {
        DataType type = DataType.map(DataType.text(), DataType.bigint());

        CellValidator cv = cellValidator(type);

        assertFalse(cv.equals(null));
        assertTrue(cv.equals(cv));
        assertFalse(cv.equals(cellValidator(DataType.timeuuid())));
        assertFalse(cv.equals(cellValidator(DataType.map(DataType.timestamp(), DataType.uuid()))));
        assertTrue(cv.equals(cellValidator(DataType.map(DataType.text(), DataType.bigint()))));
        assertEquals(DataType.Name.MAP, cv.getCqlTypeName());
    }

    public void testCellValidatorMethod() throws NoSuchFieldException {

        assertNull(cellValidator((Object) null));

        UUID uuid = UUID.fromString("AE47FBFD-A086-47C2-8C73-77D8A8E99F35");
        CellValidator cv = cellValidator(uuid);
        assertEquals(cv.getAbstractType(), UUIDType.instance);
        assertNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.NOT_A_COLLECTION);


        UUID testTimeUUID = UUID.fromString("A5C78940-9260-11E3-BAA8-0800200C9A66");
        cv = cellValidator(testTimeUUID);
        assertEquals(cv.getAbstractType(), TimeUUIDType.instance);
        assertNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.NOT_A_COLLECTION);

        BigInteger testBigInt = new BigInteger("9032809489230884980323498324376012647321674142290");
        cv = cellValidator(testBigInt);

        assertEquals(cv.getAbstractType(), IntegerType.instance);
        assertNull(cv.getValidatorTypes());
        assertEquals(cv.validatorKind(), Kind.NOT_A_COLLECTION);

        Field emails = CommonsTestEntity.class.getDeclaredField("emails");

        cv = cellValidator(emails);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), SetType.class.getCanonicalName());
        assertEquals(cv.validatorKind(), CellValidator.Kind.SET);
        assertNotNull(cv.getValidatorTypes());
        assertEquals(cv.getValidatorTypes().size(), 1);

        Iterator<String> iter = cv.getValidatorTypes().iterator();
        assertEquals(iter.next(), "text");

        Field phones = CommonsTestEntity.class.getDeclaredField("phones");

        cv = cellValidator(phones);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), ListType.class.getCanonicalName());
        assertEquals(cv.validatorKind(), Kind.LIST);
        assertNotNull(cv.getValidatorTypes());
        assertEquals(cv.getValidatorTypes().size(), 1);

        iter = cv.getValidatorTypes().iterator();
        assertEquals(iter.next(), "text");

        Field uuid2id = CommonsTestEntity.class.getDeclaredField("uuid2id");

        cv = cellValidator(uuid2id);
        assertNotNull(cv);
        assertEquals(cv.getValidatorClassName(), MapType.class.getCanonicalName());
        assertEquals(cv.validatorKind(), Kind.MAP);
        assertNotNull(cv.getValidatorTypes());
        assertEquals(cv.getValidatorTypes().size(), 2);

        iter = cv.getValidatorTypes().iterator();
        assertEquals(iter.next(), "uuid");
        assertEquals(iter.next(), "int");
    }

    public void testValidatorClassToKind() {
        assertEquals(Kind.validatorClassToKind(null), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(TimeUUIDType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(UTF8Type.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(Int32Type.class), Kind.NOT_A_COLLECTION);

        assertEquals(Kind.validatorClassToKind(BooleanType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(TimestampType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(DecimalType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(LongType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(DoubleType.class), Kind.NOT_A_COLLECTION);

        assertEquals(Kind.validatorClassToKind(FloatType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(InetAddressType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(IntegerType.class), Kind.NOT_A_COLLECTION);
        assertEquals(Kind.validatorClassToKind(UUIDType.class), Kind.NOT_A_COLLECTION);

        assertEquals(Kind.validatorClassToKind(SetType.class), Kind.SET);
        assertEquals(Kind.validatorClassToKind(ListType.class), Kind.LIST);
        assertEquals(Kind.validatorClassToKind(MapType.class), Kind.MAP);
    }

    public void testObjectToKind() {
        assertEquals(Kind.objectToKind(null), Kind.NOT_A_COLLECTION);

        /* let's try with some set implementation */
        assertEquals(Kind.objectToKind(new HashSet()), Kind.SET);
        assertEquals(Kind.objectToKind(new CopyOnWriteArraySet()), Kind.SET);
        assertEquals(Kind.objectToKind(new ConcurrentSkipListSet()), Kind.SET);
        assertEquals(Kind.objectToKind(new LinkedHashSet()), Kind.SET);
        assertEquals(Kind.objectToKind(new TreeSet()), Kind.SET);

        /* let's try with some list implementation */
        assertEquals(Kind.objectToKind(new ArrayList()), Kind.LIST);
        assertEquals(Kind.objectToKind(new CopyOnWriteArrayList<>()), Kind.LIST);
        assertEquals(Kind.objectToKind(new LinkedList<>()), Kind.LIST);
        assertEquals(Kind.objectToKind(new RoleList()), Kind.LIST);
        assertEquals(Kind.objectToKind(new RoleUnresolvedList()), Kind.LIST);
        assertEquals(Kind.objectToKind(new Stack()), Kind.LIST);
        assertEquals(Kind.objectToKind(new Vector<>()), Kind.LIST);

        /* let's try with some map implementation */
        assertEquals(Kind.objectToKind(new HashMap()), Kind.MAP);
        assertEquals(Kind.objectToKind(new ConcurrentHashMap<>()), Kind.MAP);
        assertEquals(Kind.objectToKind(new ConcurrentSkipListMap<>()), Kind.MAP);
        assertEquals(Kind.objectToKind(new Hashtable<>()), Kind.MAP);
        assertEquals(Kind.objectToKind(new IdentityHashMap<>()), Kind.MAP);
        assertEquals(Kind.objectToKind(new LinkedHashMap<>()), Kind.MAP);
        assertEquals(Kind.objectToKind(new PrinterStateReasons()), Kind.MAP);
        assertEquals(Kind.objectToKind(new Properties()), Kind.MAP);
        assertEquals(Kind.objectToKind(new TreeMap<>()), Kind.MAP);
        assertEquals(Kind.objectToKind(new WeakHashMap<>()), Kind.MAP);
    }


}

