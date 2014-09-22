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
import com.stratio.deep.commons.utils.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class ExtractorConfig<T> implements Serializable {

    private Map<String, Serializable> values = new HashMap<>();

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

    public Map<String, Serializable> getValues() {
        return values;
    }

    public void setValues(Map<String, Serializable> values) {
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

    public ExtractorConfig<T> putValue(String key, Serializable value) {
        values.put(key, value);
        return this;
    }

    public String getExtractorImplClassName() {
        return extractorImplClassName;
    }

    public void setExtractorImplClassName(String extractorImplClassName) {
        this.extractorImplClassName = extractorImplClassName;
    }

    public String getString(String key){
        return getValue(String.class, key);
    }

    public Integer getInteger(String key){
        return getValue(Integer.class, key);
    }

    public Boolean getBoolean(String key){
        return getValue(Boolean.class, key);
    }

    public String[] getStringArray(String key){
        return getValue(String[].class, key);
    }

    public Double getDouble(String key){
        return getValue(Double.class, key);
    }

    public Float getFloat(String key){
        return getValue(Float.class, key);
    }

    public Long getLong(String key){
        return getValue(Long.class, key);
    }

    public Short getShort(String key){
        return getValue(Short.class, key);
    }

    public Byte[] getByteArray(String key){
        return getValue(Byte[].class, key);
    }

    public <K,V> Pair<K,V> getPair(String key, Class<K> keyClass, Class<V> valueClass){
        return getValue(Pair.class, key);
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

    /**
     * Returns the cell value casted to the specified class.
     *
     * @param clazz the expected class
     * @param <T>   the return type
     * @return the cell value casted to the specified class
     */
    public <T> T getValue(Class<T> clazz, String key) {
        if (values.get(key) == null) {
            return null;
        } else {
            return (T) values.get(key);
        }
    }
}

