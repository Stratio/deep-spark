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
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.MessageTestEntity;

/**
 * Created by rcrespo on 9/09/14.
 */

/**
 * This is the common test that validate each extractor.
 *
 * @param <T>   the type parameter
 * @param <S>   the type parameter
 */
public abstract class ExtractorTest<T, S extends BaseConfig<T>> implements Serializable {

    /**
     * The constant LOG.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorTest.class);

    /**
     * The Input entity.
     */
    private Class inputEntity;

    /**
     * The Output entity.
     */
    private Class outputEntity;

    /**
     * The Config entity.
     */
    private Class configEntity;

    /**
     * The Host.
     */
    private final String host;

    /**
     * The Port.
     */
    private Integer port;

    private Integer port2 = 9360;

    /**
     * The Database.
     */
    protected String database = "test";

    /**
     * The Table read.
     */
    protected final String tableRead = "input";

    /**
     * The constant READ_COUNT_EXPECTED.
     */
    private static final long READ_COUNT_EXPECTED = 1l;

    /**
     * The constant READ_FIELD_EXPECTED.
     */
    private static final String READ_FIELD_EXPECTED = "new message test";

    private static final String ID_MESSAGE_EXPECTED = "messageTest";

    /**
     * The Extractor.
     */
    protected Class<IExtractor<T, S>> extractor;

    /**
     * The Origin book.
     */
    protected T originBook;

    /**
     * The constant BOOK_INPUT.
     */
    protected static final String BOOK_INPUT = "bookinput";

    /**
     * The constant BOOK_OUTPUT.
     */
    protected static final String BOOK_OUTPUT = "bookoutput";

    /**
     * The WORD _ cOUNT _ sPECTED.
     */
    protected Long WORD_COUNT_SPECTED = 3833L;

    private static final String DATA_TEST_DIVINE_COMEDY = "/divineComedy.json";

    private static final String DATA_TEST_MESSAGE = "/message.json";

    private String customDataSet;

    /**
     * The Database extractor name.
     */
    protected String databaseExtractorName ;

    /**
     * Instantiates a new Extractor test.
     *
     * @param extractor the extractor
     * @param host the host
     * @param port the port
     * @param isCells the is cells
     */
    public ExtractorTest(Class<IExtractor<T, S>> extractor, String host, Integer port, boolean isCells) {
        this(extractor, host, port, isCells, null);
    }
    public ExtractorTest(Class<IExtractor<T, S>> extractor, String host, Integer port, boolean isCells,
                         Class dataSetClass) {
        if (isCells) {
            this.inputEntity = Cells.class;
            this.outputEntity = Cells.class;
            this.configEntity = Cells.class;
        } else {
            this.inputEntity = MessageTestEntity.class;
            this.outputEntity = MessageTestEntity.class;
            if(dataSetClass!=null){
                this.configEntity = dataSetClass;
            }else{
                this.configEntity = BookEntity.class;
            }

        }

        this.host = host;
        this.port = port;
        this.extractor = extractor;
        this.databaseExtractorName = extractor.getSimpleName().toLowerCase();
    }


    /**
     * Read file.
     *
     * @param path the path
     * @return the list
     */
    protected List<String> readFile(String path){
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

    /**
     * Transform RDD.
     *
     * @param <T>   the type parameter
     * @param stringJavaRDD the string java rDD
     * @param entityClass the entity class
     * @return the java rDD
     */
    protected <T> JavaRDD<T> transformRDD(JavaRDD<String> stringJavaRDD, final Class<T> entityClass){


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

    /**
     * Init data set.
     *
     * @throws IOException the iO exception
     */
    @BeforeClass(alwaysRun = true)
    public void initDataSet() throws IOException {
        DeepSparkContext context = getDeepSparkContext();



        initDataSetDivineComedy(context);

        initDataSetMessage(context);


        context.stop();
    }

    protected void initDataSetDivineComedy(DeepSparkContext context){
        JavaRDD<String> stringJavaRDD;

        //Divine Comedy
        List<String> lineas = readFile(DATA_TEST_DIVINE_COMEDY);



        stringJavaRDD = context.parallelize(lineas);

        JavaRDD<T> javaRDD =  transformRDD(stringJavaRDD, configEntity);


        originBook = javaRDD.first();

        DeepSparkContext.saveRDD(javaRDD.rdd(), (ExtractorConfig<T>) getWriteExtractorConfig(BOOK_INPUT,
                configEntity));
    }


    protected void initDataSetMessage(DeepSparkContext context){
        //Test Message

        List<String> lineas = readFile(DATA_TEST_MESSAGE);

        JavaRDD<String>  stringJavaRDD = context.parallelize(lineas);

        JavaRDD<T> javaRDD =  transformRDD(stringJavaRDD, inputEntity);



        DeepSparkContext.saveRDD(javaRDD.rdd(), (ExtractorConfig<T>) getWriteExtractorConfig(tableRead,
                inputEntity));
    }

    /**
     * Transform to T type.
     *
     * @param <W>   the type parameter
     * @param jsonObject the json object
     * @param nameSpace the name space
     * @param entityClass the entity class
     * @return the t
     */
    protected abstract <W> W transform(JSONObject jsonObject, String nameSpace, Class<W> entityClass);

    /**
     * It tests if the extractor can read from the data store
     * @param <W>   the type parameter
     */
    @Test(alwaysRun = true, groups = {"FunctionalTests"})
    public <W> void testRead() {

        DeepSparkContext context = getDeepSparkContext();

        try {

            ExtractorConfig<W> inputConfigEntity = getReadExtractorConfig(databaseExtractorName, tableRead, inputEntity);

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            Assert.assertEquals(READ_COUNT_EXPECTED, inputRDDEntity.count());

            if (inputConfigEntity.getEntityClass().isAssignableFrom(Cells.class)) {
                Assert.assertEquals(((Cells) inputRDDEntity.first()).getCellByName("message").getCellValue(), READ_FIELD_EXPECTED);

                Assert.assertEquals(((Cells) inputRDDEntity.first()).getCellByName("id").getCellValue(), ID_MESSAGE_EXPECTED);
            } else {
                Assert.assertEquals(((MessageTestEntity) inputRDDEntity.first()).getMessage(), READ_FIELD_EXPECTED);

                Assert.assertEquals(((MessageTestEntity) inputRDDEntity.first()).getId(), ID_MESSAGE_EXPECTED);
            }

        } finally {
            context.stop();
        }

    }

    /**
     * It tests if the extractor can write to the data store
     * @param <W>   the type parameter
     */
    @Test(alwaysRun = true)
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

    
    /**
     * Test input columns.
     * @param <W>   the type parameter
     */
    @Test(alwaysRun = true)
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
//TODO check this
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

    /**
     * Gets extractor config.
     *
     * @param clazz the clazz
     * @return the extractor config
     */
    private <W> ExtractorConfig<W> getExtractorConfig(Class<W> clazz) {
        return new ExtractorConfig<>(clazz);
    }

    /**
     * Test filter EQ.
     * @param <W>  the type parameter
     */
    @Test(alwaysRun = true, dependsOnGroups = {"FunctionalTests"})
    protected <W> void testFilterEQ() {
        DeepSparkContext context = getDeepSparkContext();
        try {

            Filter[] filters = null;

            Filter filter = new Filter("id", FilterType.EQ, "TestDataSet");
            filters = new Filter[] { filter };
            ExtractorConfig<W> inputConfigEntity2 = getFilterConfig(filters);

            RDD<W> inputRDDEntity2 = context.createRDD(inputConfigEntity2);
            assertEquals(inputRDDEntity2.count(), 1);
        } finally{
            context.stop();
        }

    }

    /**
     * Test filter NEQ.
     * @param <W>  the type parameter
     */
    @Test
    protected <W> void testFilterNEQ() {
        DeepSparkContext context = getDeepSparkContext();
        try {

            Filter[] filters = null;
            Filter filter = new Filter("id", FilterType.NEQ, "TestDataSet");
            filters = new Filter[] { filter };
            ExtractorConfig<W> inputConfigEntity = getFilterConfig(filters);

            RDD<W> inputRDDEntity = context.createRDD(inputConfigEntity);

            assertEquals(inputRDDEntity.count(), 0);


        } finally{
            context.stop();
        }

    }

    /**
     * Gets write extractor config.
     *
     * @param tableOutput the table output
     * @param entityClass the entity class
     * @return the write extractor config
     */
    public <W> ExtractorConfig<W> getWriteExtractorConfig(String tableOutput, Class entityClass) {
        ExtractorConfig<W> extractorConfig = getExtractorConfig(entityClass);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseExtractorName)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.PORT2, port2)
                .putValue(ExtractorConstants.COLLECTION, tableOutput)
                .putValue(ExtractorConstants.CREATE_ON_WRITE, true);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    /**
     * Gets read extractor config.
     *
     * @return the read extractor config
     */
    public <W> ExtractorConfig<W> getReadExtractorConfig() {
        return getReadExtractorConfig(database, tableRead, inputEntity);
    }

    /**
     * Gets read extractor config.
     *
     * @param database the database
     * @param collection the collection
     * @param entityClass the entity class
     * @return the read extractor config
     */
    public <W> ExtractorConfig<W> getReadExtractorConfig(String database, String collection, Class entityClass) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(entityClass);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, database)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.PORT2, port2)
                .putValue(ExtractorConstants.COLLECTION, collection);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    /**
     * Gets input column config.
     *
     * @param inputColumns the input columns
     * @return the input column config
     */
    public <W> ExtractorConfig<W> getInputColumnConfig(String... inputColumns) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(configEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseExtractorName)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.PORT2, port2)
                .putValue(ExtractorConstants.COLLECTION, BOOK_INPUT)
                .putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    /**
     * Gets filter config.
     *
     * @param filters the filters
     * @return the filter config
     */
    public <W> ExtractorConfig<W> getFilterConfig(Filter[] filters) {

        ExtractorConfig<W> extractorConfig = getExtractorConfig(configEntity);
        extractorConfig.putValue(ExtractorConstants.HOST, host)
                .putValue(ExtractorConstants.DATABASE, databaseExtractorName)
                .putValue(ExtractorConstants.COLLECTION, BOOK_INPUT)
                .putValue(ExtractorConstants.PORT, port)
                .putValue(ExtractorConstants.PORT2, port2)
                .putValue(ExtractorConstants.FILTER_QUERY, filters);
        extractorConfig.setExtractorImplClass(extractor);
        return extractorConfig;
    }

    /**
     * Is entity class cells.
     *
     * @param extractorConfig the extractor config
     * @return the boolean
     */
    private boolean isEntityClassCells(ExtractorConfig extractorConfig) {
        if (extractorConfig.getEntityClass().isAssignableFrom(Cells.class)) {
            return true;
        }
        return false;
    }

    /**
     * Get deep spark context.
     *
     * @return the deep spark context
     */
    protected static DeepSparkContext getDeepSparkContext(){
        return new DeepSparkContext("local", "deepSparkContextTest");
    }

}
