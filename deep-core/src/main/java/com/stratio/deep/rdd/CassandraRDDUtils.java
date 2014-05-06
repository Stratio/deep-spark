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

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.deep.config.GenericDeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.cql.DeepCqlRecordWriter;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.functions.AbstractSerializableFunction2;
import com.stratio.deep.utils.Utils;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by luca on 16/04/14.
 */
class CassandraRDDUtils {

    /**
     * private constructor
     */
    CassandraRDDUtils() {
    }

    static <W> void doCql3SaveToCassandra(RDD<W> rdd, IDeepJobConfig<W> writeConfig,
                                          Function1<W, Tuple2<Cells, Cells>> transformer) {
        if (!writeConfig.getIsWriteConfig()) {
            throw new IllegalArgumentException("Provided configuration object is not suitable for writing");
        }
        Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple = new Tuple2<>(null, null);

        RDD<Tuple2<Cells, Cells>> mappedRDD = rdd.map(transformer,
                ClassTag$.MODULE$.<Tuple2<Cells, Cells>>apply(tuple.getClass()));

        ((GenericDeepJobConfig) writeConfig).createOutputTableIfNeeded(mappedRDD);

        final int pageSize = writeConfig.getBatchSize();
        int offset = 0;

        List<Tuple2<Cells, Cells>> elements = Arrays.asList((Tuple2<Cells, Cells>[]) mappedRDD.collect());
        List<Tuple2<Cells, Cells>> split;
        do {
            split = elements.subList(pageSize * (offset++), Math.min(pageSize * offset, elements.size()));

            Batch batch = QueryBuilder.batch();

            for (Tuple2<Cells, Cells> t : split) {
                Tuple2<String[], Object[]> bindVars = Utils.prepareTuple4CqlDriver(t);

                Insert insert = QueryBuilder.insertInto(writeConfig.getKeyspace(), writeConfig.getTable())
                        .values(bindVars._1(), bindVars._2());

                batch.add(insert);
            }
            writeConfig.getSession().execute(batch);

        } while (!split.isEmpty() && split.size() == pageSize);
    }

    /**
     * Provided the mapping function <i>transformer</i> that transforms a generic RDD to an RDD<Tuple2<Cells, Cells>>,
     * this generic method persists the RDD to underlying Cassandra datastore.
     *
     * @param rdd
     * @param writeConfig
     * @param transformer
     */
    static <W> void doSaveToCassandra(RDD<W> rdd, final IDeepJobConfig<W> writeConfig,
                                      Function1<W, Tuple2<Cells, Cells>> transformer) {

        if (!writeConfig.getIsWriteConfig()) {
            throw new IllegalArgumentException("Provided configuration object is not suitable for writing");
        }

        Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple = new Tuple2<>(null, null);

        final RDD<Tuple2<Cells, Cells>> mappedRDD = rdd.map(transformer,
                ClassTag$.MODULE$.<Tuple2<Cells, Cells>>apply(tuple.getClass()));

        ((GenericDeepJobConfig) writeConfig).createOutputTableIfNeeded(mappedRDD);

        ClassTag<Integer> uClassTag = ClassTag$.MODULE$.apply(Integer.class);

        mappedRDD.context().runJob(mappedRDD,
                new AbstractSerializableFunction2<TaskContext, Iterator<Tuple2<Cells, Cells>>, Integer>() {

                    @Override
                    public Integer apply(TaskContext context, Iterator<Tuple2<Cells, Cells>> rows) {

                        try (DeepCqlRecordWriter writer = new DeepCqlRecordWriter(context, writeConfig)) {
                            while (rows.hasNext()) {
                                Tuple2<Cells, Cells> row = rows.next();
                                writer.write(row._1(), row._2());
                            }
                        }

                        return null;
                    }
                }, uClassTag
        );

    }
}
