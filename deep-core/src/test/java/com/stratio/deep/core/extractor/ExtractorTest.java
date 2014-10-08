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

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/**
 * Created by rcrespo on 9/09/14.
 */

/**
 * This is the common test that validate each extractor.
 * @param <T>
 */
public abstract class ExtractorTest<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorTest.class);

    Class inputEntity;

    Class outputEntity;

    Class configEntity;

//    protected DeepSparkContext context;

    private String host;

    private Integer port;

    protected String database = "test";

    protected String databaseInputColumns = "book";

    protected final String tableRead = "input";

    protected final String filter = "filter";

    private static final long READ_COUNT_EXPECTED = 1l;

    private static final String READ_FIELD_EXPECTED = "new message test";


    protected Class<IExtractor<T>> extractor;



    /**
     *
     * @param extractor
     * @param host
     * @param port
     */
    public ExtractorTest (Class<IExtractor<T>> extractor, String host, Integer port, boolean isCells){
        super();
        if(isCells){
            this.inputEntity=Cells.class;
            this.outputEntity=Cells.class;
            this.configEntity=Cells.class;
        }else{
            this.inputEntity = MessageTestEntity.class;
            this.outputEntity= MessageTestEntity.class;
            this.configEntity= BookEntity.class;
        }

        this.host=host;
        this.port=port;
        this.extractor=extractor;
    }


    /**
     * It tests if the extractor can read from the data store
     */
    @Test
    public <W>void testRead(){

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig();

        RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

        Assert.assertEquals(READ_COUNT_EXPECTED, inputRDDEntity.count());

        if(inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)){
            Assert.assertEquals(READ_FIELD_EXPECTED,((Cells)inputRDDEntity.first()).getCellByName("message").getCellValue());
        }else{


            Assert.assertEquals(READ_FIELD_EXPECTED, ((MessageTestEntity) inputRDDEntity.first()).getMessage());
        }
        context.stop();



    }


    /**
     * It tests if the extractor can write to the data store
     */
    @Test
    public <W>void testWrite(){

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig();



        RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);



        ExtractorConfig<W> outputConfigEntity;
        if(inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
            outputConfigEntity = getWriteExtractorConfig("outputCells");
        }else{
            outputConfigEntity = getWriteExtractorConfig("outputEntity");
        }


        //Save RDD in DataSource
        context.saveRDD(inputRDDEntity, outputConfigEntity);

        RDD<W> outputRDDEntity = context.createRDD(outputConfigEntity);

        if(inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)){
            Assert.assertEquals(READ_FIELD_EXPECTED,((Cells)outputRDDEntity.first()).getCellByName("message").getCellValue());
        }else{

            Assert.assertEquals(READ_FIELD_EXPECTED, ((MessageTestEntity) outputRDDEntity.first()).getMessage());
        }
        context.stop();

    }

    @Test
    public <W> void testInputColumns(){

        DeepSparkContext context = new DeepSparkContext("local", "deepSparkContextTest");

        ExtractorConfig<W> inputConfigEntity = getInputColumnConfig(new String[] {"_id", "metadata"});

        RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);


        if(isEntityClassCells(inputConfigEntity)){
            Cells bookCells = (Cells)inputRDDEntity.first();

            assertNotNull(bookCells.getCellByName("_id").getCellValue());
            assertNotNull(bookCells.getCellByName("metadata").getCellValue());
            assertNull(bookCells.getCellByName("cantos"));
        }else{
            BookEntity bookEntity = (BookEntity)inputRDDEntity.first();

            assertNotNull(bookEntity.getId());
            assertNotNull(bookEntity.getMetadataEntity());
            assertNull(bookEntity.getCantoEntities());
        }





        ExtractorConfig<W> inputConfigEntity2 = getInputColumnConfig( "cantos");


        RDD<W> inputRDDEntity2 = context.createRDD(inputConfigEntity2);



        if(isEntityClassCells(inputConfigEntity2)){
            Cells bookCells = (Cells)inputRDDEntity2.first();

            assertNull(bookCells.getCellByName("_id"));
            assertNull(bookCells.getCellByName("metadata"));
            assertNotNull(bookCells.getCellByName("cantos").getCellValue());
        }else{
            BookEntity bookEntity2 = (BookEntity)inputRDDEntity2.first();

            assertNull(bookEntity2.getId());
            assertNull(bookEntity2.getMetadataEntity());
            assertNotNull(bookEntity2.getCantoEntities());
        }





        ExtractorConfig<W> inputConfigEntity3 = getInputColumnConfig("cantos", "metadata");



        RDD<W> inputRDDEntity3 = context.createRDD(inputConfigEntity3);



        if(isEntityClassCells(inputConfigEntity3)){
            Cells bookCells = (Cells)inputRDDEntity3.first();

            assertNull(bookCells.getCellByName("_id"));
            assertNotNull(bookCells.getCellByName("metadata").getCellValue());
            assertNotNull(bookCells.getCellByName("cantos").getCellValue());
        }else{
            BookEntity bookEntity = (BookEntity)inputRDDEntity3.first();

            assertNull(bookEntity.getId());
            assertNotNull(bookEntity.getMetadataEntity());
            assertNotNull(bookEntity.getCantoEntities());
        }




        context.stop();

    }


    private <W> ExtractorConfig<W> getExtractorConfig(Class<W> clazz){
        return new ExtractorConfig<>(clazz);
    }

    @Test
    protected void testFilter(){
        assertEquals(true,true);
    }

    public <W>ExtractorConfig<W> getWriteExtractorConfig(String output) {
        ExtractorConfig<W> extractorConfig = getExtractorConfig(outputEntity);
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, output)
                .putValue(ExtractorConstants.CREATE_ON_WRITE, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public <W>ExtractorConfig<W> getReadExtractorConfig() {

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

    public <W>ExtractorConfig<W> getFilterConfig() {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(inputEntity);
        extractorConfig.setEntityClass(inputEntity);
        extractorConfig.putValue(ExtractorConstants.HOST,host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.COLLECTION,tableRead)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.FILTER_QUERY, filter);


        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    /**
     * It closes spark's context
     */

    private boolean isEntityClassCells(ExtractorConfig extractorConfig){
        if(extractorConfig.getEntityClass().isAssignableFrom(Cells.class)) {
            return true;
        }
        return false;
    }


}
