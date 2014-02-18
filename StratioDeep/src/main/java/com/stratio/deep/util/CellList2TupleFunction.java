package com.stratio.deep.util;

import scala.Tuple2;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.functions.AbstractSerializableFunction1;

/**
 * Created by luca on 04/02/14.
 */
public class CellList2TupleFunction extends AbstractSerializableFunction1<Cells, Tuple2<Cells, Cells>> {
    /**
     * 
     */
    private static final long serialVersionUID = -8057223762615779372L;

    @Override
    public Tuple2<Cells, Cells> apply(Cells cells) {
	return CassandraRDDUtils.cellList2tuple(cells);
    }
}
