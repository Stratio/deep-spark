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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.json.simple.JSONObject;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepIllegalAccessException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import static com.stratio.deep.commons.utils.CellsUtils.getCellFromJson;

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

    private final String host;

    private final Integer port;

    protected String database = "test";

    protected final String tableRead = "input";

    private static final long READ_COUNT_EXPECTED = 1l;

    private static final String READ_FIELD_EXPECTED = "new message test";

    protected Class<IExtractor<T, S>> extractor;

    protected T originBook;

    protected static final String BOOK_INPUT = "bookinput";

    protected static final String BOOK_OUTPUT = "bookoutput";

    protected Long WORD_COUNT_SPECTED = 3833L;

    protected String databaseExtractorName ;

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
        this.databaseExtractorName = extractor.getSimpleName().toLowerCase();
    }


    private List<String> readFile(String path){
        List<String> lineas = new ArrayList<>();
        try{
            InputStream in = getClass().getResourceAsStream(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String uniqueLine = reader.readLine();

            lineas.add(uniqueLine);
            reader.close();
            in.close();

        }catch (Exception e){
            e.printStackTrace();

        }
        return lineas;
    }

    private <T> JavaRDD<T> transformRDD(JavaRDD<String> stringJavaRDD, final Class<T> entityClass){


        JavaRDD<JSONObject> jsonObjectJavaRDD = stringJavaRDD.map(new Function<String, JSONObject>() {
            @Override
            public JSONObject call(String v1) throws Exception {
                return (JSONObject) JSONValue.parse(v1);

            }
        });

        JavaRDD<T> javaRDD = jsonObjectJavaRDD.map(new Function<JSONObject, T>() {
            @Override
            public T call(JSONObject v1) throws Exception {
                return transform(v1, BOOK_INPUT, entityClass);
            }
        });

        return javaRDD;
    }

    @BeforeClass
    public void initDataSet() throws IOException {
        DeepSparkContext context = getDeepSparkContext();

        JavaRDD<String> stringJavaRDD;

        //Divine Comedy
        List<String> lineas = readFile("/divineComedy.json");



        stringJavaRDD = context.parallelize(lineas);

        JavaRDD<T> javaRDD =  transformRDD(stringJavaRDD, configEntity);

        originBook = javaRDD.first();
        DeepSparkContext.saveRDD(javaRDD.rdd(), (ExtractorConfig<T>) getWriteExtractorConfig(BOOK_INPUT,
                configEntity.getClass()));


        //Test Message

        lineas = readFile("/message.json");

        stringJavaRDD = context.parallelize(lineas);

        javaRDD =  transformRDD(stringJavaRDD, inputEntity);



        DeepSparkContext.saveRDD(javaRDD.rdd(), (ExtractorConfig<T>) getWriteExtractorConfig(tableRead,
                inputEntity.getClass()));

        context.stop();
    }

    protected abstract <T> T transform(JSONObject jsonObject, String nameSpace, Class<T> entityClass);

    /**
     * It tests if the extractor can read from the data store
     */
    @Test
    public <W> void testRead() {

        DeepSparkContext context = getDeepSparkContext();

        try {

            ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig(databaseExtractorName, tableRead, inputEntity);

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            Assert.assertEquals(READ_COUNT_EXPECTED, inputRDDEntity.count());

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(((Cells) inputRDDEntity.first()).getCellByName("message").getCellValue(), READ_FIELD_EXPECTED);
            } else {

                Assert.assertEquals(((MessageTestEntity) inputRDDEntity.first()).getMessage(), READ_FIELD_EXPECTED);
            }

        } finally {
            context.stop();
        }

    }

    /**
     * It tests if the extractor can write to the data store
     */
    @Test
    public <W> void testWrite() {


        DeepSparkContext context = getDeepSparkContext();

        try {

            ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig(databaseExtractorName, tableRead, inputEntity);

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            ExtractorConfig<W> outputConfigEntity;
            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                outputConfigEntity = getWriteExtractorConfig("outputCells", Cells.class);
            } else {
                outputConfigEntity = getWriteExtractorConfig("outputEntity",MessageTestEntity.class);
            }

            // Save RDD in DataSource
            context.saveRDD(inputRDDEntity, outputConfigEntity);

            RDD<W> outputRDDEntity = context.createRDD(outputConfigEntity);

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(((Cells) outputRDDEntity.first()).getCellByName("message").getCellValue(), READ_FIELD_EXPECTED);
            } else {

                Assert.assertEquals(((MessageTestEntity) outputRDDEntity.first()).getMessage(), READ_FIELD_EXPECTED);
            }
        } finally {
            context.stop();
        }

    }

    @Test
    public <W> void testInputColumns() {

        DeepSparkContext context = getDeepSparkContext();
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

        } finally {
            context.stop();
        }

    }

    private <W> ExtractorConfig<W> getExtractorConfig(Class<W> clazz) {
        return new ExtractorConfig<>(clazz);
    }

    @Test
    protected <W> void testFilter() {
        DeepSparkContext context = getDeepSparkContext();
        try {

            Filter[] filters = null;
            Filter filter = new Filter("id", FilterType.NEQ, "TestDataSet");
            filters = new Filter[] { filter };
            ExtractorConfig<W> inputConfigEntity = getFilterConfig(filters);

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 0);

            //

            Filter filter2 = new Filter("id", FilterType.EQ, "TestDataSet");
            filters = new Filter[] { filter2 };
            ExtractorConfig<W> inputConfigEntity2 = getFilterConfig(filters);

            RDD<W> inputRDDEntity2 = context.createRDD(inputConfigEntity2);
            assertEquals(inputRDDEntity2.count(), 1);
        } finally{
            context.stop();
        }

    }

    public <W> ExtractorConfig<W> getWriteExtractorConfig(String tableOutput, Class entityClass) {
        ExtractorConfig<W> extractorConfig = getExtractorConfig(entityClass);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseExtractorName)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, tableOutput)
                .putValue(ExtractorConstants.CREATE_ON_WRITE, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public <W> ExtractorConfig<W> getReadExtractorConfig() {
        return getReadExtractorConfig(database, tableRead, inputEntity);
    }

    public <W> ExtractorConfig<W> getReadExtractorConfig(String database, String collection, Class entityClass) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(entityClass);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, collection);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }


    public <W> ExtractorConfig<W> getInputColumnConfig(String... inputColumns) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(configEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseExtractorName)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.COLLECTION, BOOK_INPUT)
                .putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    public <W> ExtractorConfig<W> getFilterConfig(Filter[] filters) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(configEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseExtractorName)
                .putValue(ExtractorConstants.COLLECTION, BOOK_INPUT)
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

    protected static DeepSparkContext getDeepSparkContext(){
        return new DeepSparkContext("local", "deepSparkContextTest");
    }

}
