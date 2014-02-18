package com.stratio.deep.util;

import scala.Tuple2;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.functions.AbstractSerializableFunction1;

/**
* Created by luca on 20/01/14.
*/
public class DeepType2TupleFunction<T extends IDeepType> extends AbstractSerializableFunction1<T, Tuple2<Cells, Cells>> {

    /**
     * 
     */
    private static final long serialVersionUID = 3701384431140105598L;

    @Override
    public Tuple2<Cells, Cells> apply(T e) {
	return CassandraRDDUtils.deepType2tuple(e);
    }
}
