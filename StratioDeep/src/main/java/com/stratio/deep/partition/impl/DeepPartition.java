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

package com.stratio.deep.partition.impl;

import com.stratio.deep.cql.DeepTokenRange;
import org.apache.spark.Partition;

/**
 * Object that carries spark's partition information.
 */
public class DeepPartition implements Partition {

    private static final int MAGIC_NUMBER = 41;

    private static final long serialVersionUID = 4822039463206513988L;

    /**
     * Id of the rdd to which this partition belongs to.
     */
    private final int rddId;

    /**
     * index of the partition.
     */
    private final int idx;

    /**
     * Cassandra's split object, maintains information of
     * the start and end token of the cassandra split mapped
     * by this partition and its list of replicas.
     */
    private final DeepTokenRange splitWrapper;

    /**
     * Public constructor.
     *
     * @param rddId
     * @param idx
     * @param range
     */
    public DeepPartition(int rddId, int idx, DeepTokenRange range) {

        this.splitWrapper = range;
        this.rddId = rddId;
        this.idx = idx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return (MAGIC_NUMBER * (MAGIC_NUMBER + this.rddId) + this.idx);
    }

    /**
     * Returns the index of the current partition.
     *
     * @return
     */
    @Override
    public int index() {
        return this.idx;
    }

    /**
     * Returns the Cassandra split
     * @return
     */
    public DeepTokenRange splitWrapper() {
        return this.splitWrapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DeepPartition{" +
                "rddId=" + rddId +
                ", idx=" + idx +
                ", splitWrapper=" + splitWrapper +
                '}';
    }
}
