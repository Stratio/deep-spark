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

package com.stratio.deep.entity;

import com.stratio.deep.exception.DeepGenericException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Represents a tuple inside the Cassandra's datastore. A Cells object basically is an
 * ordered collection of {@see Cell} objects, plus a few utility methods to access specific cells in the row.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public class Cells implements Iterable<Cell<?>>, Serializable {

    private static final long serialVersionUID = 3074521612130550380L;
    private List<Cell<?>> cells = new ArrayList<>();

    /**
     * Default constructor.
     */
    public Cells() {
    }

    /**
     * Builds a new Cells object containing the provided cells.
     *
     * @param cells
     */
    public Cells(Cell<?>... cells) {
        Collections.addAll(this.cells, cells);
    }

    /**
     * Adds a new Cell object to this Cells instance.
     * @param c
     * @return
     */
    public boolean add(Cell<?> c) {
        if (c == null) {
            throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
        }

        return cells.add(c);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * Returns the cell at position idx.
     *
     * @param idx
     * @return
     */
    public Cell<?> getCellByIdx(int idx) {
        return cells.get(idx);
    }

    /**
     * Returns the Cell whose name is cellName, or null if this Cells object
     * contains no cell whose name is cellName.
     * @param cellName
     * @return
     */
    public Cell<?> getCellByName(String cellName) {
        for (Cell<?> c : cells) {
            if (c.getCellName().equals(cellName)) {
                return c;
            }
        }
        return null;
    }

    /**
     * Returns an inmutable collection of Cell objects contained in this Cells.
     * @return
     */
    public Collection<Cell<?>> getCells() {
        return Collections.unmodifiableList(cells);
    }

    /**
     * Converts every Cell contained in this object to an ArrayBuffer.
     * In order to perform the conversion we use the appropriate Cassandra marshaller for the Cell.
     *
     * @return
     */
    public Collection<ByteBuffer> getDecomposedCellValues() {
        List<ByteBuffer> res = new ArrayList<>();

        for (Cell<?> c : cells) {
            res.add(c.getDecomposedCellValue());
        }

        return res;
    }

    /**
     * Converts every Cell contained in this object to an ArrayBuffer.
     * In order to perform the conversion we use the appropriate Cassandra marshaller for the Cell.
     *
     * @return
     */
    public Collection<Object> getCellValues() {
        List<Object> res = new ArrayList<>();

        for (Cell<?> c : cells) {
            res.add(c.getCellValue());
        }

        return res;
    }

    /**
     * Extracts from this Cells object the cells marked either as partition key or cluster key.
     * Returns an empty Cells object if the current object does not contain any Cell marked as key.
     * @return
     */
    public Cells getIndexCells() {
        Cells res = new Cells();
        for (Cell<?> cell : cells) {
            if (cell.isPartitionKey() || cell.isClusterKey()) {
                res.add(cell);
            }

        }

        return res;
    }

    /**
     * Extracts from this Cells object the cells _NOT_ marked as partition key and _NOT_ marked as cluster key.
     * @return
     */
    public Cells getValueCells() {
        Cells res = new Cells();
        for (Cell<?> cell : cells) {
            if (!cell.isPartitionKey() && !cell.isClusterKey()) {
                res.add(cell);
            }

        }

        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return cells.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Cell<?>> iterator() {
        return getCells().iterator();
    }

    /**
     * Returns the number of cell(s) this object contains.
     * @return
     */
    public int size() {
        return cells.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Cells{" + "cells=" + cells + '}';
    }
}
