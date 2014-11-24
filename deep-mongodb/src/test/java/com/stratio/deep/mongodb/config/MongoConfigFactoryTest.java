package com.stratio.deep.mongodb.config;

import static org.testng.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import com.stratio.deep.testutils.UnitTest;
import org.testng.annotations.Test;

/**
 * Created by rcrespo on 17/07/14.
 */
@Test(groups = {"UnitTests"})
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
