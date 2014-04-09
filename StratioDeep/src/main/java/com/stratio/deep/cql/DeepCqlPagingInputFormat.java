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

package com.stratio.deep.cql;

import com.stratio.deep.partition.impl.DeepPartitionLocationComparator;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@link CqlPagingRecordReader} implementation that returns an instance of a
 * {@link DeepCqlPagingRecordReader}.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepCqlPagingInputFormat extends CqlPagingInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = super.getSplits(context);

        try {
            DeepPartitionLocationComparator comparator = new DeepPartitionLocationComparator();
            for (InputSplit split : splits) {
                Arrays.sort(split.getLocations(), comparator);
            }
        } catch (InterruptedException e) {

        }

        return splits;
    }

    /**
     * Returns a new instance of {@link DeepCqlPagingRecordReader}.
     */
    public RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>> createRecordReader(InputSplit arg0,
                                                                                             DeepTaskAttemptContext arg1) throws IOException, InterruptedException {

        return new DeepCqlPagingRecordReader(arg1.getConf().getAdditionalFilters());
    }
}
