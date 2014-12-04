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

package com.stratio.es.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.es.config.ESDeepJobConfig;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(groups = { "UnitTests" })
public class CellDeepJobConfigESTest {

    @Test
    public void createTest() {
        ESDeepJobConfig<Cells> cellDeepJobConfigES = new ESDeepJobConfig(Cells.class);
        assertNotNull(cellDeepJobConfigES);
        assertEquals(cellDeepJobConfigES.getEntityClass(), Cells.class);
    }

}