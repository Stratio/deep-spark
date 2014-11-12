/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
