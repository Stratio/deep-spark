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
package com.stratio.deep.core.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import com.stratio.deep.commons.config.BaseConfig;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Created by rcrespo on 12/08/14.
 */
public class DeepJavaRDD<T, S extends BaseConfig> extends JavaRDD<T> {

    public DeepJavaRDD(DeepRDD<T, S> rdd) {
        super(rdd, ClassTag$.MODULE$.<T>apply(rdd.config.value().getEntityClass()));
    }

    @Override
    public ClassTag<T> classTag() {
        return ClassTag$.MODULE$.<T>apply(((BaseConfig<T,BaseConfig>)((DeepRDD) this.rdd()).config.value())
                .getEntityClass());
    }

}
