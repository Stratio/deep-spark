package com.stratio.deep.functions;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.util.Utils;
import scala.Tuple2;

/**
 * Function that converts an IDeepType to tuple of two Cells.<br/>
 * The first Cells element contains the list of Cell elements that represent the key (partition + cluster key). <br/>
 * The second Cells element contains all the other columns.
 */
public class DeepType2TupleFunction<T extends IDeepType> extends AbstractSerializableFunction<T, Tuple2<Cells, Cells>> {

    private static final long serialVersionUID = 3701384431140105598L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<Cells, Cells> apply(T e) {
        return Utils.deepType2tuple(e);
    }
}
