/**
 * 
 */
package com.stratio.deep.cassandra.querybuilder;

import static com.stratio.deep.commons.utils.Utils.quote;

import java.util.List;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;

/**
 *
 */

public class IncreaseCountersQueryBuilder extends CassandraUpdateQueryBuilder {

    private static final long serialVersionUID = 4324594860937682515L;

    /*
     * (non-Javadoc)
     *
     * @see com.stratio.deep.commons.querybuilder.QueryBuilder#prepareQuery()
     */
    @Override
    public String prepareQuery(Cells keys, Cells values) {

        // TODO validate values > 0

        StringBuilder sb = new StringBuilder("UPDATE ").append(getCatalogName()+"."+getTableName())
                .append(" SET ");



        boolean isFirst = true;
        for (Cell cell : values.getCells()) {
            if (!isFirst) {
                sb.append(", ");
            }else isFirst=false;

            sb.append(quote(cell.getCellName())).append(" = ").append(quote(cell.getCellName())).append(" + ")
                    .append("?");
        }

        isFirst=true;
        StringBuilder keyClause = new StringBuilder(" WHERE ");

        for (Cell cell : keys.getCells()) {
                if (!isFirst) {
                    keyClause.append(" AND ");
                }else isFirst=false;

            keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

      }
        sb.append(keyClause).append(";");

        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.querybuilder.QueryBuilder#prepareBatchQuery(java.util.List)
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
