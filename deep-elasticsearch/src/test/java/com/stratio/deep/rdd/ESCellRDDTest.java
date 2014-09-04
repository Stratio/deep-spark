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

import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

/**
 * Created by rcrespo on 29/08/14.
 */
@Test(suiteName = "ESRddTests", groups = {"ESCellRDDTest"})
public class ESCellRDDTest {
    private Logger log = Logger.getLogger(getClass());
// @Test
// public void testReadingRDD() {
//
// String hostConcat = ESJavaRDDTest.HOST.concat(":").concat(ESJavaRDDTest.PORT.toString());
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createES()
// .host(hostConcat).database(DATABASE).initialize();
//
// RDD<Cells> inputRDDEntity = context.ESRDD (inputConfigEntity);
//
//
//// assertEquals(0, inputRDDEntity.cache().count());
//// assertEquals(ESJavaRDDTest.col.findOne().get("message"), inputRDDEntity.first().getCellByName("message").getCellValue());
//
// context.stop();
//
// }
//
// @Test
// public void testWritingRDD() {
//
//
// String hostConcat = HOST.concat(":").concat(PORT.toString());
//
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createES()
// .host(hostConcat).database(DATABASE).initialize();
//
// RDD<Cells> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
// IESDeepJobConfig<Cells> outputConfigEntity = ESConfigFactory.createES()
// .host(hostConcat).database(DATABASE_OUTPUT).initialize();
//
//
// //Save RDD in ESDB
// ESCellRDD.saveCell(inputRDDEntity, outputConfigEntity);
//
// RDD<Cells> outputRDDEntity = context.ESRDD(outputConfigEntity);
//
//
// assertEquals(ESJavaRDDTest. ES.getDB(DATABASE).getCollection(COLLECTION_OUTPUT_CELL).findOne().get("message"),
// outputRDDEntity.first().getCellByName("message").getCellValue());
//
//
// context.stop();
//
// }
// @Test
// public void testTransform() {
//
//
// String hostConcat = HOST.concat(":").concat(PORT.toString());
//
// ESDeepSparkContext context = new ESDeepSparkContext("local", "deepSparkContextTest");
//
// IESDeepJobConfig<Cells> inputConfigEntity = ESConfigFactory.createESDB()
// .host(hostConcat).database(DATABASE).collection(COLLECTION_INPUT).initialize();
//
// RDD<Cells> inputRDDEntity = context.ESRDD(inputConfigEntity);
//
// try{
// inputRDDEntity.first();
// fail();
// }catch (DeepTransformException e){
// // OK
// log.info("Correctly catched DeepTransformException: " + e.getLocalizedMessage());
// }
//
// context.stop();
//
// }
}