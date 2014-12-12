package com.stratio.deep.jdbc.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.stratio.deep.commons.entity.Cells;
import org.testng.annotations.Test;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class CellDeepJobConfigJdbcTest {

    @Test
    public void createTest() {
        JdbcDeepJobConfig<Cells> config = new JdbcDeepJobConfig<>(Cells.class);
        assertNotNull(config);
        assertEquals(config.getEntityClass(), Cells.class);
    }

}
