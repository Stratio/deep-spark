/*
 * Copyright 2014, Stratio.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.stratio.deep.rdd;

import java.nio.ByteBuffer;
import java.util.Map;


import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.CassandraCell;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.utils.Pair;

/**
 * Concrete implementation of a CassandraRDD representing an RDD of
 * {@link com.stratio.deep.entity.Cells} element.<br/>
 */
public class CassandraCellRDD extends CassandraRDD<Cells> {

  private static final long serialVersionUID = -738528971629963221L;



  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Cells transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem,
      IDeepJobConfig<Cells, ? extends IDeepJobConfig<?, ?>> config) {

    Cells cells = new Cells(((ICassandraDeepJobConfig) config).getTable());
    Map<String, Cell> columnDefinitions = config.columnDefinitions();


    for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
      Cell cd = columnDefinitions.get(entry.getKey());
      cells.add(CassandraCell.create(cd, entry.getValue()));
    }

    for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
      Cell cd = columnDefinitions.get(entry.getKey());
      if (cd == null) {
        continue;
      }

      cells.add(CassandraCell.create(cd, entry.getValue()));
    }

    return cells;
  }


}
