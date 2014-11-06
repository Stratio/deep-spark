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
public abstract class QueryBuilder implements Serializable {

    protected String keyspace;

    protected String columnFamily;

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
    public abstract String prepareQuery(Cells keys, Cells values);

    /**
     * Returns a CQL batch query wrapping the given statements.
     * 
     * @param statements
     *            the list of statements to use to generate the batch statement.
     * @return the batch statement.
     */
    public abstract String prepareBatchQuery(List<String> statements);

    public void setKeyspace(String keyspace){
        this.keyspace = keyspace;

    }

    public void setColumnFamily(String columnFamily){
        this.columnFamily = columnFamily;

    }
}
