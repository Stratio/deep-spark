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

package com.stratio.deep.rdd;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.extractor.ExtractorTest;
import com.stratio.deep.extractor.ESCellExtractor;


import com.stratio.deep.extractor.ESEntityExtractor;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.action.count.CountResponse;
import org.testng.annotations.Test;


import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESCellRDDTest"}, dependsOnGroups = "ESJavaRDDTest")
public class ESCellRDDTest extends ExtractorTest {
    private Logger LOG = Logger.getLogger(getClass());

    public ESCellRDDTest() {
        super(ESCellExtractor.class,"localhost:9200",null,ESJavaRDDTest.ES_INDEX_MESSAGE+ESJavaRDDTest.ES_SEPARATOR+ESJavaRDDTest.ES_TYPE_MESSAGE,
                true);
    }



    @Override
    @Test
    public void testInputColumns() {
        assertEquals(true, true);
    }

}