/**
 * 
 */
package com.stratio.deep.cassandra.functions;

import com.stratio.deep.cassandra.cql.DeepCqlRecordWriter;
import com.stratio.deep.cassandra.cql.DeepIncrementalCqlRecordWriter;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.functions.SaveFunction;

import java.util.Set;

/**
 *
 */
public class IncreaseCounterFunction extends CassandraSaveFunction {

    private DeepIncrementalCqlRecordWriter writer;
    private Set<String> primaryKeys;
    private Set<String> targetColumns;

    public IncreaseCounterFunction(Set<String> primaryKeys, Set<String> targetColumns){
        this.primaryKeys = primaryKeys;
        this.targetColumns = targetColumns;
    }
    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.functions.SaveFunction#call()
     */
    @Override
    public void call(Cells row) {
        Cells keys = new Cells(row.getnameSpace());
        for (String primaryKey : primaryKeys){
            keys.add(row.getCellByName(row.getnameSpace(),primaryKey));
        }
        Cells values = new Cells(row.getnameSpace());
        for (String targetColumn : targetColumns){
            values.add(row.getCellByName(row.getnameSpace(),targetColumn));
        }

        writer.write(keys,values);
    }

    public void setWriter(DeepCqlRecordWriter writer){

        this.writer=(DeepIncrementalCqlRecordWriter) writer;
    }


}
