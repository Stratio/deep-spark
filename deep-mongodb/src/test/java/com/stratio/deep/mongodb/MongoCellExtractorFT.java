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

package com.stratio.deep.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.stratio.deep.core.extractor.ExtractorCellTest;
import com.stratio.deep.mongodb.extractor.MongoCellExtractor;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test(suiteName = "mongoRddTests", groups = { "MongoCellExtractorTest", "FunctionalTests" },
        dependsOnGroups = "MongoJavaRDDTest")
public class MongoCellExtractorFT extends ExtractorCellTest {

    private static final Logger LOG = LoggerFactory.getLogger(MongoCellExtractorFT.class);

    public MongoCellExtractorFT() {
        super(MongoCellExtractor.class, "localhost", 27890, true);
    }

}
