package com.stratio.deep.jdbc.config;

import com.stratio.deep.commons.entity.Cells;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Created by mariomgal on 26/01/15.
 */
@Test(groups = { "UnitTests" })
public class CellDeepJobConfigJdbcNeo4JTest {

    @Test
    public void createTest() {
        JdbcNeo4JDeepJobConfig<Cells> config = new JdbcNeo4JDeepJobConfig<>(Cells.class);
        assertNotNull(config);
        assertEquals(config.getEntityClass(), Cells.class);
    }
}
