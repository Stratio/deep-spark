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

package com.stratio.deep.config;


import com.stratio.deep.entity.Cells;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class ExtractorConfig<T> implements Serializable {

    private Map<String, String> values = new HashMap<>();

    private Class ExtractorImplClass;


    private Class entityClass = Cells.class;


    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }

    public Class getExtractorImplClass() {
        return ExtractorImplClass;
    }

    public void setExtractorImplClass(Class extractorImplClass) {
        ExtractorImplClass = extractorImplClass;
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public ExtractorConfig<T> putValue(String key, String value) {
        values.put(key, value);
        return this;
    }
}
