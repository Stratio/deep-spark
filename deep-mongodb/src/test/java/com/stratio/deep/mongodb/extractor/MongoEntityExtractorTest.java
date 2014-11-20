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

package com.stratio.deep.mongodb.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.stratio.deep.core.extractor.ExtractorEntityTest;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test(suiteName = "mongoRddTests", groups = { "MongoEntityExtractorTest" }, dependsOnGroups = "MongoCellExtractorTest")
public class MongoEntityExtractorTest extends ExtractorEntityTest {

    private static final Logger LOG = LoggerFactory.getLogger(MongoEntityExtractorTest.class);

    public MongoEntityExtractorTest() {
        super(MongoEntityExtractor.class, "localhost:27890", null, false);
    }

}
