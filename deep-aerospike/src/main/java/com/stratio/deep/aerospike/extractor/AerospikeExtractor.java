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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.hadoop.mapreduce.AerospikeInputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeKey;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.config.AerospikeDeepOutputFormat;
import com.stratio.deep.commons.extractor.impl.GenericHadoopExtractor;

import java.io.Serializable;

public abstract class AerospikeExtractor<T> extends GenericHadoopExtractor<T, AerospikeDeepJobConfig<T>,
        AerospikeKey, AerospikeRecord, Object, AerospikeRecord> implements Serializable {

    private static final long serialVersionUID = -5537656625573650019L;

    public AerospikeExtractor(){
        super();
        this.inputFormat = new AerospikeInputFormat();
        this.outputFormat = new AerospikeDeepOutputFormat();
    }

}
