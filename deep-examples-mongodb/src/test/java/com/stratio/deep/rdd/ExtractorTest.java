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

package com.stratio.deep.rdd;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.rdd.IExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


/**
 * Created by rcrespo on 9/09/14.
 */
//@Test
public abstract class ExtractorTest<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorTest.class);

    T t;

    private String host;

    private Integer port;

    protected final String database = "test";

    protected final String tableWrite = "output";

    protected final String tableRead = "input";

    protected final String filter = "filter";

    protected final String inputColumns = "field1, field2, field3";


    protected Class<IExtractor<T>> extractor;

    public ExtractorTest (Class<IExtractor<T>> extractor, String host, Integer port){
        super();
        this.host=host;
        this.port=port;
        this.extractor=extractor;
    }

    @Test
    protected abstract void testRead();

    @Test
    protected abstract void testWrite();

    @Test
    protected abstract void testInputColumns();

    @Test
    protected abstract void testFilter();

    public ExtractorConfig getWriteExtractorConfig(Class entity) {
        ExtractorConfig<T> extractorConfig = new ExtractorConfig<>(entity);
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION, tableWrite);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public ExtractorConfig getReadExtractorConfig(Class entity) {

        ExtractorConfig extractorConfig = new ExtractorConfig<>(entity);
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION,tableRead);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public ExtractorConfig getInputColumnConfig(Class entity) {

        ExtractorConfig extractorConfig = new ExtractorConfig<>(entity);
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION,tableRead)
                .putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public ExtractorConfig getFilterConfig() {

        ExtractorConfig extractorConfig = new ExtractorConfig<>(t.getClass());
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION,tableRead)
                .putValue(ExtractorConstants.FILTER_QUERY,filter);


        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }
}
