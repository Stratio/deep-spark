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

import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Writable;
import org.apache.spark.Partition;
import org.apache.spark.SerializableWritable;

public class DeepPartition implements Partition {

    private static final int MAGIC_NUMBER = 41;

    private static final long serialVersionUID = 4822039463206513988L;

    private final int rddId;
    private final int idx;
    private final SerializableWritable<ColumnFamilySplit> splitWrapper;

    public DeepPartition(int rddId, int idx, Writable s) {

        this.splitWrapper = new SerializableWritable<>((ColumnFamilySplit) s);
        this.rddId = rddId;
        this.idx = idx;
    }

    @Override
    public int hashCode() {
        return (MAGIC_NUMBER * (MAGIC_NUMBER + this.rddId) + this.idx);
    }

    @Override
    public int index() {
        return this.idx;
    }

    public SerializableWritable<ColumnFamilySplit> splitWrapper() {
        return this.splitWrapper;
    }

    @Override
    public String toString() {
        return "DeepPartition [rddId="
            + rddId
            + ", idx="
            + idx
            + ", "
            + (splitWrapper != null ? "startToken=" + splitWrapper.value().getStartToken() : "")
            + (splitWrapper != null ? ", endToken=" + splitWrapper.value().getEndToken() : "")
            + (splitWrapper != null ? ", locations=" + ArrayUtils.toString(splitWrapper.value().getLocations())
            : "") + "]";
    }

}
