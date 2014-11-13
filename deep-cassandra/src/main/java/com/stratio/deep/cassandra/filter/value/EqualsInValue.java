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

    private final Serializable equalsValue;

    private final List<Serializable> inValues;

    public EqualsInValue(Serializable equalsValue, List<Serializable> inValues) {
        super();
        this.equalsValue = equalsValue;
        this.inValues = inValues;
    }

    public Serializable getEqualsValue() {
        return equalsValue;
    }

    public List<Serializable> getInValues() {
        return inValues;
    }
}
