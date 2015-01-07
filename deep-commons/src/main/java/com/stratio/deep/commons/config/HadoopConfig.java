/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.commons.config;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.stratio.deep.commons.entity.Cells;

/**
 * Created by rcrespo on 13/10/14.
 */
public class HadoopConfig<T, S extends DeepJobConfig<T, S>> extends DeepJobConfig<T, S> implements Serializable {

    private static final long serialVersionUID = -3800967311704235141L;
    
    protected transient Configuration configHadoop;

    public HadoopConfig(Class<T> t) {
        super(t);
    }

    public HadoopConfig(){
        super((Class<T>) Cells.class);
    }

    public Configuration getHadoopConfiguration() {
        return configHadoop;
    }

}
