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

package com.stratio.deep.core.function;

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.core.extractor.client.ExtractorClient;
import org.apache.spark.api.java.function.Function2;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

import static com.stratio.deep.utils.Constants.SPARK_PARTITION_ID;

/**
 * Created by rcrespo on 28/08/14.
 */
public class PrepareSaveFunction<T> extends AbstractFunction1<Iterator<T>, BoxedUnit> implements Serializable {

    private ExtractorConfig<T> extractorConfig;

    private T first;



    public PrepareSaveFunction(ExtractorConfig<T> extractorConfig, T first){
        this.first=first;
        this.extractorConfig=extractorConfig;
    }


    @Override
    public BoxedUnit apply(Iterator<T> v1) {
        ExtractorClient<T> extractorClient = new ExtractorClient<T>();
        extractorClient.initialize();
        extractorClient.initSave(extractorConfig, first);
        while(v1.hasNext()){
            extractorClient.saveRDD(v1.next());
        }
        extractorConfig.putValue(SPARK_PARTITION_ID, String.valueOf(Integer.parseInt(extractorConfig.getValues().get(SPARK_PARTITION_ID))+1)) ;
        extractorClient.close();
        return null;
    }



}
