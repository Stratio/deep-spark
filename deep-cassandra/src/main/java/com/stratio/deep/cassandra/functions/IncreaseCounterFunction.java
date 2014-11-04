/**
 * 
 */
package com.stratio.deep.cassandra.functions;

import com.stratio.deep.cassandra.cql.DeepCqlRecordWriter;
import com.stratio.deep.commons.functions.SaveFunction;

/**
 *
 */
public class IncreaseCounterFunction implements SaveFunction {

    private DeepCqlRecordWriter writer;

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.functions.SaveFunction#call()
     */
    @Override
    public void call() {

    }

}
