package com.stratio.deep.aerospike.config;

import com.stratio.deep.commons.entity.Cells;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Created by mariomgal on 01/11/14.
 */
@Test
public class CellDeepJobConfigAerospikeTest {

    @Test
    public void createTest() {

        AerospikeDeepJobConfig<Cells> cellDeepJobConfigAerospike = new AerospikeDeepJobConfig<>(Cells.class);

        assertNotNull(cellDeepJobConfigAerospike);

        assertEquals(cellDeepJobConfigAerospike.getEntityClass(), Cells.class);

    }
}
