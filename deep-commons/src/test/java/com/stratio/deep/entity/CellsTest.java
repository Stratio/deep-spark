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

package com.stratio.deep.entity;

import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepGenericExceptionTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test
public class CellsTest {

    @Test
    public void testInstantiation() {

        Cells cells = new Cells();
        assertEquals(cells.getDefaultTableName(), "3fa2fbc6d8abbc77cdab9e3216d957dffd64a64b");


        cells = new Cells("defaultTable");
        assertEquals(cells.getDefaultTableName(), "defaultTable");

        assertEquals(cells.size(), 0);

    }

    @Test
    public void testAdd() {

        Cells cells = new Cells("defaultTable");

        try {
            cells.add(null);

            fail();
        } catch (DeepGenericException dge) {
            // ok
        } catch (Exception e) {
            fail();
        }


    }
}
