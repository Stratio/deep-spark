/**
 * 
 */
package com.stratio.deep.cassandra.filter.value;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class EqualsInValue implements Serializable {

    private static final long serialVersionUID = -5152237867344382113L;

    private final String equalsField;

    private final Serializable equalsValue;

    private final String inField;

    private final List<Serializable> inValues;

    public EqualsInValue(String equalsField, Serializable equalsValue, String inField, List<Serializable> inValues) {
        super();
        this.equalsField = equalsField;
        this.equalsValue = equalsValue;
        this.inField = inField;
        this.inValues = inValues;
    }

    public String getEqualsField() {
        return equalsField;
    }

    public Serializable getEqualsValue() {
        return equalsValue;
    }

    public String getInField() {
        return inField;
    }

    public List<Serializable> getInValues() {
        return inValues;
    }
}
