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

package com.stratio.es;


import com.stratio.deep.core.extractor.ExtractorCellTest;
import com.stratio.deep.core.extractor.ExtractorTest;
import com.stratio.deep.es.extractor.ESCellExtractor;

import com.stratio.deep.testutils.FunctionalTest;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;




/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = { "ESCellRDDTest", "FunctionalTests" }, dependsOnGroups = "ESJavaRDDTest")
public class ESCellRDDFT extends ExtractorCellTest {

    public ESCellRDDFT() {
        super(ESCellExtractor.class,"localhost",9200, true);
    }


}