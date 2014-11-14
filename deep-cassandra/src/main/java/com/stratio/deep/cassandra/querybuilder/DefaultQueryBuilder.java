/**
 * 
 */
package com.stratio.deep.cassandra.querybuilder;

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
public class DefaultQueryBuilder extends CassandraUpdateQueryBuilder {

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.querybuilder.QueryBuilder#prepareQuery()
     */
    @Override
    public String prepareQuery(Cells keys, Cells values) {

        StringBuilder sb = new StringBuilder("UPDATE ").append(getCatalogName()).append(".").append(getTableName())
                .append(" SET ");

        boolean isFirst = true;
        StringBuilder keyClause = new StringBuilder(" WHERE ");

        for (Cell cell : keys.getCells()) {
                if (!isFirst) {
                    keyClause.append(" AND ");
                }else isFirst=false;
                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));
        }

        isFirst=true;

        for (Cell cell : values.getCells()) {
            if (!isFirst) {
                sb.append(", ");
            }else isFirst=false;
            sb.append(String.format("%s = ?", quote(cell.getCellName())));

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
