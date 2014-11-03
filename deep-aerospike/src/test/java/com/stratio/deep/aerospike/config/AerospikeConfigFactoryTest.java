package com.stratio.deep.aerospike.config;

import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import static org.testng.Assert.assertTrue;

/**
 * Created by mariomgal on 01/11/14.
 */
@Test
public class AerospikeConfigFactoryTest {

    @Test(expectedExceptions = InvocationTargetException.class)
    public void testConstructorIsPrivate()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<AerospikeConfigFactory> constructor = AerospikeConfigFactory.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
}
