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

package com.stratio.deep.entity;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.stratio.deep.exception.DeepGenericException;

/**
 * Represents a tuple inside the Cassandra's datastore. A Cells object basically is an ordered
 * collection of {@link com.stratio.deep.entity.Cell} objects, plus a few utility methods to access
 * specific cells in the row.
 * 
 * @author Luca Rosellini <luca@stratio.com>
 */
public class Cells implements Iterable<Cell>, Serializable {

  private static final long serialVersionUID = 3074521612130550380L;
  private List<Cell> cells = new ArrayList<>();

  /**
   * Default constructor.
   */
  public Cells() {}

  /**
   * Builds a new Cells object containing the provided cells.
   * 
   * @param cells the array of Cells we want to use to create the Cells object.
   */
  public Cells(Cell... cells) {
    Collections.addAll(this.cells, cells);
  }

  /**
   * Adds a new Cell object to this Cells instance.
   * 
   * @param c the Cell we want to add to this Cells object.
   * @return either true/false if the Cell has been added successfully or not.
   */
  public boolean add(Cell c) {
    if (c == null) {
      throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
    }

    return cells.add(c);
  }

  /**
   * Replaces the cell having the same name that the given one with the given Cell object.
   * 
   * @param c the Cell to replace the one in the Cells object.
   * @return either true/false if the Cell has been successfully replace or not.
   */
  public boolean replaceByName(Cell c) {

    if (c == null) {
      throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
    }

    boolean cellFound = false;
    int position = 0;

    Iterator<Cell> cellsIt = cells.iterator();
    while (!cellFound && cellsIt.hasNext()) {
      Cell currentCell = cellsIt.next();

      if (currentCell.getCellName().equals(c.getCellName())) {
        cellFound = true;
      } else {
        position++;
      }
    }

    if (cellFound) {
      cells.remove(position);

      return cells.add(c);
    }

    return false;
  }

  /**
   * Removes the cell with the given cell name.
   * 
   * @param cellName the name of the cell to be removed.
   * @return either true/false if the Cell has been successfully removed or not.
   */
  public boolean remove(String cellName) {

    if (cellName == null) {
      throw new DeepGenericException(new IllegalArgumentException(
          "cell name parameter cannot be null"));
    }

    Iterator<Cell> cellsIt = cells.iterator();

    while (cellsIt.hasNext()) {
      Cell currentCell = cellsIt.next();

      if (currentCell.getCellName().equals(cellName)) {
        return cells.remove(currentCell);
      }
    }

    return false;
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

    for (Cell cell : cells) {
      Cell otherCell = o.getCellByName(cell.getCellName());

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
   * @param idx the index position of the Cell we want to retrieve.
   * @return Returns the cell at position idx.
   */
  public Cell getCellByIdx(int idx) {
    return cells.get(idx);
  }

  /**
   * Returns the Cell whose name is cellName, or null if this Cells object contains no cell whose
   * name is cellName.
   * 
   * @param cellName the name of the Cell we want to retrieve from this Cells object.
   * @return the Cell whose name is cellName contained in this Cells object. null if no cell named
   *         cellName is present.
   */
  public Cell getCellByName(String cellName) {
    for (Cell c : cells) {
      if (c.getCellName().equals(cellName)) {
        return c;
      }
    }
    return null;
  }

  /**
   * @return Returns an immutable collection of Cell objects contained in this Cells.
   */
  public Collection<Cell> getCells() {
    return Collections.unmodifiableList(cells);
  }

  /**
   * Converts every Cell contained in this object to an ArrayBuffer. In order to perform the
   * conversion we use the appropriate Cassandra marshaller for the Cell.
   * 
   * @return a collection of Cell(s) values converted to byte buffers using the appropriate
   *         marshaller.
   */
  public Collection<ByteBuffer> getDecomposedCellValues() {
    List<ByteBuffer> res = new ArrayList<>();

    for (Cell c : cells) {
      res.add(c.getDecomposedCellValue());
    }

    return res;
  }

  /**
   * Converts every Cell contained in this object to an ArrayBuffer. In order to perform the
   * conversion we use the appropriate Cassandra marshaller for the Cell.
   * 
   * @return a collection of Cell(s) values.
   */
  public Collection<Object> getCellValues() {
    List<Object> res = new ArrayList<>();

    for (Cell c : cells) {
      res.add(c.getCellValue());
    }

    return res;
  }

  /**
   * Extracts from this Cells object the cells marked either as partition key or cluster key.
   * Returns an empty Cells object if the current object does not contain any Cell marked as key.
   * 
   * @return the Cells object containing the subset of this Cells object of only the Cell(s) part of
   *         the key.
   */
  public Cells getIndexCells() {
    Cells res = new Cells();
    for (Cell cell : cells) {
      if (cell.isPartitionKey() || cell.isClusterKey()) {
        res.add(cell);
      }

    }

    return res;
  }

  /**
   * Extracts from this Cells object the cells _NOT_ marked as partition key and _NOT_ marked as
   * cluster key.
   * 
   * @return the Cells object containing the subset of this Cells object of only the Cell(s) that
   *         are NOT part of the key.
   */
  public Cells getValueCells() {
    Cells res = new Cells();
    for (Cell cell : cells) {
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
  public Iterator<Cell> iterator() {
    return getCells().iterator();
  }

  /**
   * Returns the number of cell(s) this object contains.
   * 
   * @return the number os Cell objects contained in this Cells object.
   */
  public int size() {
    return cells.size();
  }

  /**
   * @return true if this object contains no cells.
   */
  public boolean isEmpty() {
    return cells.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Cells{" + "cells=" + cells + '}';
  }
}
