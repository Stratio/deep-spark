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

package com.stratio.deep.commons.entity;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by rcrespo on 2/07/14.
 */
public abstract class Cell implements Serializable {

    private static final long serialVersionUID = 2298549804049316156L;

    /**
     * Name of the cell. Mapped to a DataBase column name.
     */
    protected String cellName;

    /**
     * Cell value.
     */
    protected Object cellValue;

    protected Cell() {
        super();
    }

    protected Cell(String cellName, Object cellValue) {
        super();
        this.cellName = cellName;
        this.cellValue = cellValue;
    }

    public String getCellName() {
        return cellName;
    }

    public Object getCellValue() {
        return cellValue;
    }

    /**
     * @return true is the current cell is a key inside the datastore, false otherwise.
     */
    public abstract Boolean isKey();

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Cell{");
        sb.append("cellName='").append(cellName).append('\'');
        sb.append(", cellValue=").append(cellValue);
        sb.append('}');
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public ByteBuffer getDecomposedCellValue() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Cell cell = (Cell) o;

        boolean isCellName = this.getCellName().equals(cell.getCellName());
        boolean isCellValue = this.getCellValue().equals(cell.getCellValue());

        return isCellName && isCellValue;
    }

}
