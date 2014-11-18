/**
 * 
 */
package com.stratio.deep.cassandra.filter.value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.stratio.deep.commons.utils.Pair;

/**
 *
 */
public class EqualsInValue implements Serializable {

    private static final long serialVersionUID = -5152237867344382113L;

    private List<Pair<String, Serializable>> equalsList = new ArrayList<>();

    private String inField;

    private List<Serializable> inValues;

    public EqualsInValue() {
        super();
    }

    public EqualsInValue(List<Pair<String, Serializable>> equalsList, String inField, List<Serializable> inValues) {
        super();
        this.equalsList = equalsList;
        this.inField = inField;
        this.inValues = inValues;
    }

    public List<Pair<String, Serializable>> getEqualsList() {
        return equalsList;
    }

    public String getInField() {
        return inField;
    }

    public List<Serializable> getInValues() {
        return inValues;
    }

    public EqualsInValue equalsPair(String field, Serializable value) {
        Pair<String, Serializable> equalsPair = Pair.create(field, value);
        this.equalsList.add(equalsPair);
        return this;
    }

    public EqualsInValue inField(String field) {
        this.inField = field;
        return this;
    }

    public EqualsInValue inValues(List<Serializable> values) {
        this.inValues = values;
        return this;
    }
}
