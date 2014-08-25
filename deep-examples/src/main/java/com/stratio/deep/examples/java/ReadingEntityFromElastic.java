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

package com.stratio.deep.examples.java;


import org.apache.log4j.Logger;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Example class to read an entity from elastic
 */
public final class ReadingEntityFromElastic {
    private static final Logger LOG = Logger.getLogger(ReadingEntityFromElastic.class);
    public static List<Tuple2<String, Integer>> results;

    private ReadingEntityFromElastic() {
    }


    public static void main(String[] args) throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        doMain(args);
    }


    public static void doMain(String[] args) throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        String job = "java:readingEntityFromES";

        String host = "localhost:9200";

        String database = "twitter/tweet";
        String inputCollection = "input";

        //TODO Review ES* classes not fount in project
        // Creating the Deep Context where args are Spark Master and Job Name
       /* ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());


        DeepJobConfig<Cells> inputConfigEntity =
                ESConfigFactory.createES()
                        .filterQuery("?q=user:ricardo")
                        .database(database)
                        .host(host)
                        .initialize();

        RDD<Cells> inputRDDEntity = deepContext.createRDD(inputConfigEntity);


        List<Cells> list = new ArrayList<>();
        list.add(new Cells(ESCell.create("user", "ricardo")));

        RDD<Cells> rrdYo = deepContext.parallelize(list).rdd();

	    LOG.info("count : " + inputRDDEntity.cache().count());

//        LOG.info("count : " + );

        Cells cells = inputRDDEntity.first();
//
//        for(Cell cell : cells.getCells()){
//            LOG.info(cell.getCellName());

//            LOG.info(cell.getCellValue());
//            LOG.info(cell.getCellValue().getClass());
//
//        }

        LOG.info("count : "+ cells);
//        JSONObject json = UtilES.getJsonFromCell(cells);


//        deepContext.saveRDD(rrdYo, inputConfigEntity);












        IESDeepJobConfig<TweetES> inputConfigEntity2 =
                ESConfigFactory.createES(TweetES.class)
//                        .filterQuery("?q=_id:1")
//                        .database(database)
                        .database("tweet/prueba")
                        .host(host)
                        .initialize();

        RDD<TweetES> inputRDDEntity2 = deepContext.createRDD(inputConfigEntity2);

        IESDeepJobConfig<TweetES> outputConfigEntity2 =
                ESConfigFactory.createES(TweetES.class)
//                        .filterQuery("?q=_id:1")
                        .database("tweet/prueba")
                        .host(host)
                        .initialize();



        LOG.info("count : " + inputRDDEntity2.count());

        LOG.info("count : " + inputRDDEntity2.first());

        deepContext.saveRDD(inputRDDEntity2, outputConfigEntity2);
        deepContext.stop();*/
    }
}
