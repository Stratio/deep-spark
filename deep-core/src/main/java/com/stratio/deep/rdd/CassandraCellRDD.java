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

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.utils.Pair;
import org.apache.spark.SparkContext;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Concrete implementation of a CassandraRDD representing an RDD of {@link com.stratio.deep.entity.Cells} element.<br/>
 */
public class CassandraCellRDD extends CassandraRDD<Cells> {

    private static final long serialVersionUID = -738528971629963221L;

    /**
     * This constructor should not be called explicitly.<br/>
     * Use {@link com.stratio.deep.context.DeepSparkContext} instead to create an RDD.
     *
     * @param sc
     * @param config
     */
    public CassandraCellRDD(SparkContext sc, IDeepJobConfig<Cells> config) {
        super(sc, config);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Cells transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem) {

        Cells cells = new Cells();
        Map<String, Cell> columnDefinitions = config.value().columnDefinitions();

        for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
            Cell cd = columnDefinitions.get(entry.getKey());
            cells.add(Cell.create(cd, entry.getValue()));
        }

        for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
            Cell cd = columnDefinitions.get(entry.getKey());
            if (cd == null) {
                continue;
            }

            cells.add(Cell.create(cd, entry.getValue()));
        }

        return cells;
    }
}
