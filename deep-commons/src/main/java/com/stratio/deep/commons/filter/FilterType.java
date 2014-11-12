/**
 * 
 */
package com.stratio.deep.commons.filter;

/**
 *
 */
public enum FilterType {
    EQ("EQ"), GT("GT"), GET("GET"), LT("LT"), LET("LET"), NEQ("NEQ"), MATCH("MATCH"), IN("IN"), BETWEEN("BETWEEN");

    private final String filterTypeId;

    FilterType(String filterTypeId) {
        this.filterTypeId = filterTypeId;
    }

    public String getFilterTypeId() {
        return filterTypeId;
    }
}
