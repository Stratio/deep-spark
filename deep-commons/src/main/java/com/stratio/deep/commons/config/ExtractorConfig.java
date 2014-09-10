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

package com.stratio.deep.commons.config;


import com.stratio.deep.commons.entity.Cells;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class ExtractorConfig<T> implements Serializable {

    private Map<String, String> values = new HashMap<>();

    private Class extractorImplClass;

    private String extractorImplClassName;

    private Class entityClass;

    public ExtractorConfig (Class<T> t){
        super();
        entityClass = t;
    }

    public ExtractorConfig(){
        entityClass = Cells.class;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }

    public Class getExtractorImplClass() {
        return extractorImplClass;
    }

    public void setExtractorImplClass(Class extractorImplClass) {
        this.extractorImplClass = extractorImplClass;
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

    public String getExtractorImplClassName() {
        return extractorImplClassName;
    }

    public void setExtractorImplClassName(String extractorImplClassName) {
        this.extractorImplClassName = extractorImplClassName;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ExtractorConfig{");
        sb.append("values=").append(values);
        sb.append(", extractorImplClass=").append(extractorImplClass);
        sb.append(", extractorImplClassName='").append(extractorImplClassName).append('\'');
        sb.append(", entityClass=").append(entityClass);
        sb.append('}');
        return sb.toString();
    }
}

