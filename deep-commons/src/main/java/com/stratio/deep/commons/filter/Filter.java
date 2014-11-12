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

package com.stratio.deep.commons.filter;

import java.io.Serializable;

/**
 * Created by rcrespo on 2/10/14.
 */
public class Filter implements Serializable {

    private static final long serialVersionUID = -3245502101559144688L;

    private final String field;

    private FilterType filterType;

    private Serializable value;

    public Filter(String field) {
        this.field = field;
    }

    public Filter(String field, FilterType filterType, Serializable value) {
        this.field = field;
        this.filterType = filterType;
        this.value = value;
    }

    public Filter greaterThan(Serializable value) {
        this.filterType = FilterType.GT;
        this.value = value;
        return this;
    }

    public Filter greaterThanEquals(Serializable value) {
        this.filterType = FilterType.GET;
        this.value = value;
        return this;

    }

    public Filter lessThan(Serializable value) {
        this.filterType = FilterType.LT;
        this.value = value;
        return this;
    }

    public Filter lessThanEquals(Serializable value) {
        this.filterType = FilterType.LET;
        this.value = value;
        return this;
    }

    public Filter notEquals(Serializable value) {
        this.filterType = FilterType.NEQ;
        this.value = value;
        return this;
    }

    public Filter is(Serializable value) {
        this.filterType = FilterType.EQ;
        this.value = value;
        return this;
    }

    public Filter match(Serializable value) {
        this.filterType = FilterType.MATCH;
        this.value = value;
        return this;
    }

    public Filter in(Serializable value) {
        this.filterType = FilterType.IN;
        this.value = value;
        return this;
    }

    public String getField() {
        return field;
    }

    public FilterType getFilterType() {
        return filterType;
    }

    public Serializable getValue() {
        return value;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Filter{");
        sb.append("field='").append(field).append('\'');
        sb.append(", operation='").append(filterType).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
