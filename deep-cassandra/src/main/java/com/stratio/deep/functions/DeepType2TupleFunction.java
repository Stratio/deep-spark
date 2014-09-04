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

package com.stratio.deep.functions;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import scala.Tuple2;

import static com.stratio.deep.rdd.CassandraRDDUtils.deepType2tuple;

/**
 * Function that converts an IDeepType to tuple of two Cells.<br/>
 * The first Cells element contains the list of Cell elements that represent the key (partition + cluster key). <br/>
 * The second Cells element contains all the other columns.
 */
public class DeepType2TupleFunction<T extends IDeepType> extends AbstractSerializableFunction<T, Tuple2<Cells, Cells>> {

    private static final long serialVersionUID = 3701384431140105598L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<Cells, Cells> apply(T e) {
        return deepType2tuple(e);
    }
}
