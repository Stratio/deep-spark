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

package com.stratio.deep.core.extractor;

import static junit.framework.TestCase.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.Serializable;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterOperator;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;



/**
 * Created by rcrespo on 9/09/14.
 */

/**
 * This is the common test that validate each extractor.
 *
 * @param <T>
 */
public abstract class ExtractorTest<T, S extends BaseConfig<T>> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorTest.class);

    private Class inputEntity;

    private Class outputEntity;

    private Class configEntity;

    private String host;

    private Integer port;

    protected String database = "test";

    protected String databaseInputColumns = "book";

    protected final String tableRead = "input";

    private static final long READ_COUNT_EXPECTED = 1l;

    private static final String READ_FIELD_EXPECTED = "new message test";

    protected Class<IExtractor<T, S>> extractor;



    /**
     * @param extractor
     * @param host
     * @param port
     */
    public ExtractorTest(Class<IExtractor<T, S>> extractor, String host, Integer port, boolean isCells) {
        super();
        if (isCells) {
            this.inputEntity = Cells.class;
            this.outputEntity = Cells.class;
            this.configEntity = Cells.class;
        } else {
            this.inputEntity = MessageTestEntity.class;
            this.outputEntity = MessageTestEntity.class;
            this.configEntity = BookEntity.class;
        }

        this.host = host;
        this.port = port;
        this.extractor = extractor;
    }


    /**
     * It tests if the extractor can read from the data store
     */
    @Test
    public <W> void testRead() {

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig();

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            Assert.assertEquals(READ_COUNT_EXPECTED, inputRDDEntity.count());

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(READ_FIELD_EXPECTED,
                        ((Cells) inputRDDEntity.first()).getCellByName("message").getCellValue());
            } else {

                Assert.assertEquals(READ_FIELD_EXPECTED, ((MessageTestEntity) inputRDDEntity.first()).getMessage());
            }

        }finally {
            context.stop();
        }



    }

    /**
     * It tests if the extractor can write to the data store
     */
    @Test
    public <W> void testWrite() {

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        try {

            ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig();

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            ExtractorConfig<W> outputConfigEntity;
            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                outputConfigEntity = getWriteExtractorConfig("outputCells");
            } else {
                outputConfigEntity = getWriteExtractorConfig("outputEntity");
            }

            //Save RDD in DataSource
            context.saveRDD(inputRDDEntity, outputConfigEntity);

            RDD<W> outputRDDEntity = context.createRDD(outputConfigEntity);

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(READ_FIELD_EXPECTED,
                        ((Cells) outputRDDEntity.first()).getCellByName("message").getCellValue());
            } else {

                Assert.assertEquals(READ_FIELD_EXPECTED, ((MessageTestEntity) outputRDDEntity.first()).getMessage());
            }
        }finally {
            context.stop();
        }


    }

    @Test
    public <W> void testInputColumns() {

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {

            ExtractorConfig<W> inputConfigEntity = getInputColumnConfig(new String[] { "id", "metadata" });

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            if (isEntityClassCells(inputConfigEntity)) {
                Cells bookCells = (Cells) inputRDDEntity.first();

                assertNotNull(bookCells.getCellByName("id").getCellValue());
                assertNotNull(bookCells.getCellByName("metadata").getCellValue());
                assertNull(bookCells.getCellByName("cantos"));
            } else {
                BookEntity bookEntity = (BookEntity) inputRDDEntity.first();

                assertNotNull(bookEntity.getId());
                assertNotNull(bookEntity.getMetadataEntity());
                assertNull(bookEntity.getCantoEntities());
            }

            ExtractorConfig<W> inputConfigEntity2 = getInputColumnConfig("cantos");

            RDD<W> inputRDDEntity2 = context.createRDD(inputConfigEntity2);

            if (isEntityClassCells(inputConfigEntity2)) {
                Cells bookCells = (Cells) inputRDDEntity2.first();

                assertNull(bookCells.getCellByName("id"));
                assertNull(bookCells.getCellByName("metadata"));
                assertNotNull(bookCells.getCellByName("cantos").getCellValue());
            } else {
                BookEntity bookEntity2 = (BookEntity) inputRDDEntity2.first();

                assertNull(bookEntity2.getId());
                assertNull(bookEntity2.getMetadataEntity());
                assertNotNull(bookEntity2.getCantoEntities());
            }

            ExtractorConfig<W> inputConfigEntity3 = getInputColumnConfig("cantos", "metadata");

            RDD<W> inputRDDEntity3 = context.createRDD(inputConfigEntity3);

            if (isEntityClassCells(inputConfigEntity3)) {
                Cells bookCells = (Cells) inputRDDEntity3.first();

                assertNull(bookCells.getCellByName("id"));
                assertNotNull(bookCells.getCellByName("metadata").getCellValue());
                assertNotNull(bookCells.getCellByName("cantos").getCellValue());
            } else {
                BookEntity bookEntity = (BookEntity) inputRDDEntity3.first();

                assertNull(bookEntity.getId());
                assertNotNull(bookEntity.getMetadataEntity());
                assertNotNull(bookEntity.getCantoEntities());
            }

        }finally {
            context.stop();
        }



    }


    private <W> ExtractorConfig<W> getExtractorConfig(Class<W> clazz){
        return new ExtractorConfig<>(clazz);
    }

    @Test
    protected <W> void testFilter(){
        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");
        try {

            Filter[] filters = null;
            Filter filter = new Filter("id", FilterOperator.NE, "TestDataSet");
            filters = new Filter[] { filter };
            ExtractorConfig<W> inputConfigEntity = getFilterConfig(filters);

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 0);

            //

            Filter filter2 = new Filter("id", FilterOperator.IS, "TestDataSet");
            filters = new Filter[] { filter2 };
            ExtractorConfig<W> inputConfigEntity2 = getFilterConfig(filters);

            RDD<W> inputRDDEntity2 = context.createRDD(inputConfigEntity2);
            assertEquals(inputRDDEntity2.count(), 1);
        }catch(Exception e){
            e.printStackTrace();

        } finally{
            context.stop();
        }

    }

    public <W> ExtractorConfig<W> getWriteExtractorConfig(String output) {
        ExtractorConfig<W> extractorConfig = getExtractorConfig(outputEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, output)
                .putValue(ExtractorConstants.CREATE_ON_WRITE, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public <W> ExtractorConfig<W> getReadExtractorConfig() {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(inputEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, tableRead);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public <W>ExtractorConfig<W> getInputColumnConfig(String... inputColumns) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(configEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseInputColumns)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, tableRead)
                .putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public <W>ExtractorConfig<W> getFilterConfig(Filter[] filters) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(configEntity);
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, databaseInputColumns)
                .putValue(ExtractorConstants.COLLECTION,tableRead)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.FILTER_QUERY, filters);

        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    private boolean isEntityClassCells(ExtractorConfig extractorConfig) {
        if (extractorConfig.getEntityClass().isAssignableFrom(Cells.class)) {
            return true;
        }
        return false;
    }

}
