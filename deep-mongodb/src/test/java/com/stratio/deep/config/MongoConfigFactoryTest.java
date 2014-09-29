package com.stratio.deep.config;

import static org.testng.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import org.testng.annotations.Test;

import com.stratio.deep.mongodb.config.MongoConfigFactory;

/**
 * Created by rcrespo on 17/07/14.
 */
@Test
public class MongoConfigFactoryTest {

    @Test(expectedExceptions = InvocationTargetException.class)
    public void testConstructorIsPrivate()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<MongoConfigFactory> constructor = MongoConfigFactory.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
}
