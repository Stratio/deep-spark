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

import com.stratio.deep.config.ExtractorConfig;
import org.apache.spark.Partition;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T> extends Serializable {


    Partition[] getPartitions(ExtractorConfig<T> config);

    boolean hasNext();

    T next();

    void close();

    void initIterator(Partition dp, ExtractorConfig<T> config);


    IExtractor<T> getExtractorInstance(ExtractorConfig<T> config);

    void saveRDD(RDD<T> rdd, ExtractorConfig<T> config);

}
