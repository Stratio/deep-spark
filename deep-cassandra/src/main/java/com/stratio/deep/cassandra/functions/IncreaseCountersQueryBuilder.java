/**
 * 
 */
package com.stratio.deep.cassandra.functions;

import static com.stratio.deep.commons.utils.Utils.quote;

import java.util.List;

import com.stratio.deep.cassandra.entity.CassandraCell;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.functions.QueryBuilder;

/**
 *
 */
public class IncreaseCountersQueryBuilder implements QueryBuilder {

    private final String keyspace;

    private final String columnFamily;

    private final String namespace;

    public IncreaseCountersQueryBuilder(String keyspace, String columnFamily) {

        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.namespace = keyspace + "." + columnFamily;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.functions.SaveFunction#call()
     */
    @Override
    public String prepareQuery(Cells keys, Cells values) {

        StringBuilder sb = new StringBuilder("UPDATE ").append(keyspace).append(".").append(columnFamily)
                .append(" SET ");

        int k = 0;

        StringBuilder keyClause = new StringBuilder(" WHERE ");
        for (Cell cell : keys.getCells(namespace)) {
            if (((CassandraCell) cell).isPartitionKey() || ((CassandraCell) cell).isClusterKey()) {
                if (k > 0) {
                    keyClause.append(" AND ");
                }

                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

                ++k;
            }

        }

        k = 0;
        for (Cell cell : values.getCells(namespace)) {
            if (k > 0) {
                sb.append(", ");
            }

            sb.append(quote(cell.getCellName())).append(" = ").append(quote(cell.getCellName())).append(" + ")
                    .append("?");
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
        StringBuilder sb = new StringBuilder("BEGIN COUNTER \n");

        for (String statement : statements) {
            sb.append(statement).append("\n");
        }

        sb.append(" APPLY BATCH;");

        return sb.toString();
    }

}
