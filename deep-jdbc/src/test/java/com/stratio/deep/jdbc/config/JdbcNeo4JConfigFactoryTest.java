package com.stratio.deep.jdbc.config;

import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import static org.testng.Assert.assertTrue;

/**
 * Created by mariomgal on 26/01/15.
 */
@Test(groups = { "UnitTests" })
public class JdbcNeo4JConfigFactoryTest {

    @Test(expectedExceptions = {InvocationTargetException.class})
    public void testPrivateConstructor() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<JdbcNeo4JConfigFactory> constructor = JdbcNeo4JConfigFactory.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }

}
