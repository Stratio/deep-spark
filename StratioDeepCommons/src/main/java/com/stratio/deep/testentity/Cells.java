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

package com.stratio.deep.testentity;

import com.stratio.deep.exception.DeepGenericException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Represents a tuple inside the Cassandra's datastore.
 * Provides utility methods to access specific cells in the row.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public class Cells implements Iterable<Cell<?>>, Serializable {

    private static final long serialVersionUID = 3074521612130550380L;
    private List<Cell<?>> cells = new ArrayList<>();

    public Cells() {
    }

    public Cells(Cell<?>... cells) {
        Collections.addAll(this.cells, cells);
    }

    public boolean add(Cell<?> c) {
        if (c == null) {
            throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
        }

        return cells.add(c);
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof Cells)) {
            return false;
        }

        Cells o = (Cells) obj;

        if (cells.size() != this.size()) {
            return false;
        }

        for (Cell<?> cell : cells) {
            Cell<?> otherCell = o.getCellByName(cell.getCellName());

            if (otherCell == null) {
                return false;
            }

            if (!otherCell.equals(cell)) {
                return false;
            }
        }

        return true;
    }

    public Cell<?> getCellByIdx(int idx) {
        return cells.get(idx);
    }

    public Cell<?> getCellByName(String cellName) {
        for (Cell<?> c : cells) {
            if (c.getCellName().equals(cellName)) {
                return c;
            }
        }
        return null;
    }

    public Collection<Cell<?>> getCells() {
        return Collections.unmodifiableList(cells);
    }

    public Collection<ByteBuffer> getDecomposedCellValues() {
        List<ByteBuffer> res = new ArrayList<>();

        for (Cell<?> c : cells) {
            res.add(c.getDecomposedCellValue());
        }

        return res;
    }

    public Cells getIndexCells() {
        Cells res = new Cells();
        for (Cell<?> cell : cells) {
            if (cell.isPartitionKey() || cell.isClusterKey()) {
                res.add(cell);
            }

        }

        return res;
    }

    public Cells getValueCells() {
        Cells res = new Cells();
        for (Cell<?> cell : cells) {
            if (!cell.isPartitionKey() && !cell.isClusterKey()) {
                res.add(cell);
            }

        }

        return res;
    }

    @Override
    public int hashCode() {
        return cells.hashCode();
    }

    @Override
    public Iterator<Cell<?>> iterator() {
        return getCells().iterator();
    }

    public int size() {
        return cells.size();
    }

    @Override
    public String toString() {
        return "Cells{" + "cells=" + cells + '}';
    }
}
