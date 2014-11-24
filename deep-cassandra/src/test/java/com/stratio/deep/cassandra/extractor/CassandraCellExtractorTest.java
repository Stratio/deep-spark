/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.cassandra.extractor;

import static com.stratio.deep.commons.utils.CellsUtils.getCellFromJson;
import static com.stratio.deep.commons.utils.CellsUtils.getCellWithMapFromJson;

import java.lang.reflect.InvocationTargetException;

import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import com.stratio.deep.cassandra.extractor.CassandraCellExtractor;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.core.extractor.ExtractorCellTest;

/**
 * Created by rcrespo on 20/11/14.
 */
@Test(suiteName = "cassandraRddTests", groups = { "CassandraCellExtractorTest" })
public class CassandraCellExtractorTest extends ExtractorCellTest {

    private static final long serialVersionUID = -5201767473793541285L;

    public CassandraCellExtractorTest() {
        super(CassandraCellExtractor.class, "localhost", 9042, true);
    }


    @Override
    protected Cells transform(JSONObject jsonObject, String nameSpace, Class entityClass) {
        try {
            return getCellWithMapFromJson(jsonObject, "book.input");
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DeepTransformException(e.getMessage());
        }
    }
}
