/**
 * 
 */
package com.stratio.deep.commons.filter;

/**
 *
 */
public enum FilterType {
    EQ("EQ"), GT("GT"), GTE("GTE"), LT("LT"), LTE("LTE"), NEQ("NE"), MATCH("MATCH"), IN("IN"), BETWEEN("BETWEEN");

    private final String filterTypeId;

    FilterType(String filterTypeId) {
        this.filterTypeId = filterTypeId;
    }

    public String getFilterTypeId() {
        return filterTypeId;
    }
}
