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

package com.stratio.deep.cassandra.config;

import com.stratio.deep.commons.filter.FilterType;

/**
 * Created by rcrespo on 6/10/14.
 */
public enum OperatorCassandra {

    IS(FilterType.EQ, "="),
    GT(FilterType.GT, ">"),
    GTE(FilterType.GTE, ">="),
    LT(FilterType.LT, "<"),
    LTE(FilterType.LTE, "<="),
    NE(FilterType.NEQ, "!=");

    private FilterType filterType;

    private String operator;

    OperatorCassandra(FilterType filterType, String operator) {
        this.filterType = filterType;
        this.operator = operator;
    }

    public static OperatorCassandra getOperatorCassandra(FilterType filterType) {

        switch (filterType) {
        case EQ:
            return OperatorCassandra.IS;
        case GT:
            return OperatorCassandra.GT;
        case GTE:
            return OperatorCassandra.GTE;
        case LT:
            return OperatorCassandra.LT;
        case LTE:
            return OperatorCassandra.LTE;
        case NEQ:
            return OperatorCassandra.NE;
        default:
            return null;
        }

    }

    public FilterType getFilterType() {
        return filterType;
    }

    public String getOperator() {
        return operator;
    }

}
