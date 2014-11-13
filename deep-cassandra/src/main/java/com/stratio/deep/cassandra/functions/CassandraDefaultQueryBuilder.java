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
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;

/**
 *
 */
public class CassandraDefaultQueryBuilder extends UpdateQueryBuilder {


    protected final String namespace;

    public CassandraDefaultQueryBuilder(String keyspace, String columnFamily) {
        super(keyspace,columnFamily, null, null);
        this.namespace = keyspace + "." + columnFamily;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.querybuilder.QueryBuilder#prepareQuery()
     */
    @Override
    public String prepareQuery(Cells keys, Cells values) {

        StringBuilder sb = new StringBuilder("UPDATE ").append(catalogName).append(".").append(tableName)
                .append(" SET ");

        int k = 0;

        StringBuilder keyClause = new StringBuilder(" WHERE ");
        for (Cell cell : keys.getCells()) {
            if (((CassandraCell) cell).isPartitionKey() || ((CassandraCell) cell).isClusterKey()) {
                if (k > 0) {
                    keyClause.append(" AND ");
                }

                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

                ++k;
            }

        }

        k = 0;
        for (Cell cell : values.getCells()) {
            if (k > 0) {
                sb.append(", ");
            }

            sb.append(String.format("%s = ?", quote(cell.getCellName())));
            ++k;
        }

        sb.append(keyClause).append(";");

        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.querybuilder.QueryBuilder#prepareBatchQuery()
     */
    @Override
    public String prepareBatchQuery(List<String> statements) {
        StringBuilder sb = new StringBuilder("BEGIN BATCH \n");

        for (String statement : statements) {
            sb.append(statement).append("\n");
        }

        sb.append(" APPLY BATCH;");

        return sb.toString();
    }
}
