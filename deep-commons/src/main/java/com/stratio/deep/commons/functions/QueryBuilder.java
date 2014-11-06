/**
 * 
 */
package com.stratio.deep.commons.functions;

import java.io.Serializable;
import java.util.List;

import com.stratio.deep.commons.entity.Cells;

/**
 *
 */
public interface QueryBuilder extends Serializable {

    /**
     * Generates a query from the given set of keys and set of values.
     * 
     * @param keys
     *            the row keys wrapped inside a Cells object.
     * @param values
     *            all the other row columns involved in query wrapped inside a Cells object.
     * 
     * @return the query statement.
     */
    public String prepareQuery(Cells keys, Cells values);

    /**
     * Returns a CQL batch query wrapping the given statements.
     * 
     * @param statements
     *            the list of statements to use to generate the batch statement.
     * @return the batch statement.
     */
    public String prepareBatchQuery(List<String> statements);
}
