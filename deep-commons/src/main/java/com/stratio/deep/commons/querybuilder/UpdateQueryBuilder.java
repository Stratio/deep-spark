/**
 * 
 */
package com.stratio.deep.commons.querybuilder;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.stratio.deep.commons.entity.Cells;

/**
 *
 */
public abstract class UpdateQueryBuilder implements Serializable {


    protected final String catalogName;
    protected final String tableName;
    protected final Set<String> primaryKeys;
    protected final Set<String> targetFields;

    public UpdateQueryBuilder(String catalogName, String tableName, Set<String> primaryKeys, Set<String> targetFields){
        this.catalogName=catalogName;
        this.tableName = tableName;
        this.primaryKeys=primaryKeys;
        this.targetFields=targetFields;
    }
    /**
     *
     *
     * @param keys
     * @param values
     *
     *
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


    public String getCatalogName() {
        return catalogName;
    }

    public Set<String> getTargetFields() {
        return targetFields;
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableName() {
        return tableName;
    }

}
