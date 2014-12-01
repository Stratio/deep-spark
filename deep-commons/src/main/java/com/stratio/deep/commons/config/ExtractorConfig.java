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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.utils.Pair;
import com.stratio.deep.commons.utils.Utils;

/**
 * Created by rcrespo on 19/08/14.
 *
 * @param <T> the type parameter
 */
public class ExtractorConfig<T> extends BaseConfig<T> implements Serializable, Cloneable {

    private static final long serialVersionUID = -8418401138615339258L;

    private Map<String, Serializable> values = new HashMap<>();

    /**
     * Instantiates a new Extractor config.
     *
     * @param t the t
     */
    public ExtractorConfig(Class<T> t) {
        super(t);
    }

    /**
     * Instantiates a new Extractor config.
     */
    public ExtractorConfig() {
        super();
    }

    /**
     * Gets values.
     *
     * @return the values
     */
    public Map<String, Serializable> getValues() {
        return values;
    }

    /**
     * Sets values.
     *
     * @param values the values
     */
    public void setValues(Map<String, Serializable> values) {
        this.values = values;
    }

    /**
     * Put value.
     *
     * @param key   the key
     * @param value the value
     * @return the extractor config
     */
    public ExtractorConfig<T> putValue(String key, Serializable value) {
        values.put(key, value);
        return this;
    }

    @Override
    public String getExtractorImplClassName() {
        return extractorImplClassName;
    }

    @Override
    public void setExtractorImplClassName(String extractorImplClassName) {
        this.extractorImplClassName = extractorImplClassName;
    }

    /**
     * Gets string.
     *
     * @param key the key
     * @return the string
     */
    public String getString(String key) {
        return getValue(String.class, key);
    }

    /**
     * Gets integer.
     *
     * @param key the key
     * @return the integer
     */
    public Integer getInteger(String key) {
        try {
            return getValue(Integer.class, key);
        } catch (ClassCastException e) {
            return Integer.parseInt(getString(key));
        }
    }

    /**
     * Gets boolean.
     *
     * @param key the key
     * @return the boolean
     */
    public Boolean getBoolean(String key) {
        return getValue(Boolean.class, key);
    }

    /**
     * Get string array.
     *
     * @param key the key
     * @return the string [ ]
     */
    public String[] getStringArray(String key) {
        try {
            return getValue(String[].class, key);
        } catch (ClassCastException e) {
            return new String[] { getString(key) };
        }

    }

    /**
     * Gets double.
     *
     * @param key the key
     * @return the double
     */
    public Double getDouble(String key) {
        return getValue(Double.class, key);
    }

    /**
     * Gets float.
     *
     * @param key the key
     * @return the float
     */
    public Float getFloat(String key) {
        return getValue(Float.class, key);
    }

    /**
     * Gets long.
     *
     * @param key the key
     * @return the long
     */
    public Long getLong(String key) {
        return getValue(Long.class, key);
    }

    /**
     * Gets short.
     *
     * @param key the key
     * @return the short
     */
    public Short getShort(String key) {
        return getValue(Short.class, key);
    }

    /**
     * Get byte array.
     *
     * @param key the key
     * @return the byte [ ]
     */
    public Byte[] getByteArray(String key) {
        return getValue(Byte[].class, key);
    }

    /**
     * Get filter array.
     *
     * @param key the key
     * @return the filter [ ]
     */
    public Filter[] getFilterArray(String key) {
        return getValue(Filter[].class, key);
    }

    /**
     * Gets pair.
     *
     * @param key        the key
     * @param keyClass   the key class
     * @param valueClass the value class
     * @return the pair
     */
    public <K, V> Pair<K, V> getPair(String key, Class<K> keyClass, Class<V> valueClass) {
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
     * @param key   the key
     * @return the cell value casted to the specified class
     */
    public <W> W getValue(Class<W> clazz, String key) {
        if (values.get(key) == null) {
            return null;
        } else {
            try {
                return (W) values.get(key);
            } catch (ClassCastException e) {
                if (Number.class.isAssignableFrom(clazz)) {
                    try {
                        return (W) Utils.castNumberType(values.get(key), clazz.newInstance());
                    } catch (InstantiationException | IllegalAccessException e1) {
                        return null;
                    }
                } else {
                    throw e;
                }

            }

        }
    }

    @Override
    public ExtractorConfig<T> clone() {

        ExtractorConfig<T> clonedObject = new ExtractorConfig<>();
        clonedObject.getValues().putAll(this.getValues());
        clonedObject.setExtractorImplClass(this.getExtractorImplClass());
        clonedObject.setExtractorImplClassName(this.getExtractorImplClassName());
        clonedObject.setEntityClass(this.getEntityClass());
        return clonedObject;

    }
}
