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
