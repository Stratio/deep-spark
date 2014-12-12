package com.stratio.deep.jdbc.config;

import static org.testng.Assert.fail;

import com.stratio.deep.commons.entity.Cells;
import org.testng.annotations.Test;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class GenericConfigFactoryJdbcTest {

    private static final String HOST = "localhost";

    private static final int PORT = 3306;

    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private static final String DATABASE = "test";

    private static final String TABLE = "table";

    @Test
    public void testDatabaseValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).port(PORT).driverClass(DRIVER_CLASS).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.database(DATABASE);
        }
        config.initialize();
    }

    @Test
    public void testDriverClassValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).port(PORT).database(DATABASE).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.driverClass(DRIVER_CLASS);
        }
        config.initialize();
    }

    @Test
    public void testTableValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).port(PORT).driverClass(DRIVER_CLASS).database(DATABASE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.table(TABLE);
        }
        config.initialize();
    }

    @Test
    public void testHostValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.port(PORT).driverClass(DRIVER_CLASS).database(DATABASE).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.host(HOST);
        }
        config.initialize();
    }

    @Test
    public void testPortValidation() {
        JdbcDeepJobConfig<Cells> config = JdbcConfigFactory.createJdbc();
        config.host(HOST).driverClass(DRIVER_CLASS).database(DATABASE).table(TABLE);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.port(PORT);
        }
        config.initialize();
    }

}
