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

import com.stratio.deep.commons.filter.FilterOperator;

/**
 * Created by rcrespo on 6/10/14.
 */
public enum OperatorCassandra {


    OR(FilterOperator.OR, "or"),
    AND(FilterOperator.AND, "and"),
    IS(FilterOperator.IS, "="),
    GT(FilterOperator.GT, ">"),
    GTE(FilterOperator.GTE, ">="),
    LT(FilterOperator.LT, "<"),
    LTE(FilterOperator.LTE, "<="),
    NE(FilterOperator.NE, "!=");

    private String operatorName;

    private String operator;

    OperatorCassandra (String operatorName, String operator){
        this.operatorName = operatorName;
        this.operator = operator;
    }

    public static OperatorCassandra getOperatorCassandra (String operatorName){
        switch (operatorName){

        case FilterOperator.OR:
                return OperatorCassandra.OR;
        case FilterOperator.AND:
            return OperatorCassandra.AND;
        case FilterOperator.IS:
            return OperatorCassandra.IS;
        case FilterOperator.GT:
            return OperatorCassandra.GT;
        case FilterOperator.GTE:
            return OperatorCassandra.GTE;
        case FilterOperator.LT:
            return OperatorCassandra.LT;
        case FilterOperator.LTE:
            return OperatorCassandra.LTE;
        case FilterOperator.NE:
            return OperatorCassandra.NE;
        default:
            return null;
        }

    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getOperator() {
        return operator;
    }


}
