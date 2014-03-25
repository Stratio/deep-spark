/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.functions;

import com.stratio.deep.entity.Cells;
import com.stratio.deep.util.Utils;
import scala.Tuple2;

/**
 * Function that converts a Cells element to tuple of two Cells.<br/>
 * The first Cells element contains the list of Cell elements that represent the key (partition + cluster key). <br/>
 * The second Cells element contains all the other columns.
 */
public class CellList2TupleFunction extends AbstractSerializableFunction<Cells, Tuple2<Cells, Cells>> {
    private static final long serialVersionUID = -8057223762615779372L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<Cells, Cells> apply(Cells cells) {
        return Utils.cellList2tuple(cells);
    }
}
