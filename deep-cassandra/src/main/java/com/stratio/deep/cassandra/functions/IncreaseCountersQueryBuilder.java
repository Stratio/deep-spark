/**
 * 
 */
package com.stratio.deep.cassandra.functions;

import static com.stratio.deep.commons.utils.Utils.quote;

import java.util.List;
import java.util.Set;

import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.functions.QueryBuilder;

/**
 *
 */
public class IncreaseCountersQueryBuilder extends QueryBuilder {

    public IncreaseCountersQueryBuilder(){

    }



    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.functions.SaveFunction#call()
     */
    @Override
    public String prepareQuery(Cells keys, Cells values) {
        //TODO validate values > 0

        final String namespace = keyspace + "." + columnFamily;

        final String originNamespace = keys.getnameSpace();

        StringBuilder sb = new StringBuilder("UPDATE ").append(namespace)
                .append(" SET ");



        int k = 0;
        for (Cell cell : values.getCells(originNamespace)) {
            if (k > 0) {
                sb.append(", ");
            }

            sb.append(quote(cell.getCellName())).append(" = ").append(quote(cell.getCellName())).append(" + ")
                    .append("?");
            ++k;
        }

        k = 0;

        StringBuilder keyClause = new StringBuilder(" WHERE ");
        for (Cell cell : keys.getCells(originNamespace)) {
                if (k > 0) {
                    keyClause.append(" AND ");
                }

                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

                ++k;

        }
        sb.append(keyClause).append(";");

        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.functions.QueryBuilder#prepareBatchQuery(java.util.List)
     */
    @Override
    public String prepareBatchQuery(List<String> statements) {
        StringBuilder sb = new StringBuilder("BEGIN COUNTER BATCH\n");

        for (String statement : statements) {
            sb.append(statement).append("\n");
        }

        sb.append(" APPLY BATCH;");

        return sb.toString();
    }

}
