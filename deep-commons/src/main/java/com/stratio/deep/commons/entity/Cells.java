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

package com.stratio.deep.commons.entity;

import com.stratio.deep.commons.exception.DeepGenericException;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * <p>
 * Represents a tuple inside the Cassandra's datastore. A Cells object basically is an ordered
 * collection of {@link Cell} objects, plus a few utility methods to access
 * specific cells in the row.
 * </p>
 * <p>
 * A Cells object may contain Cell objects belonging to different tables. Ex: A Cells object may
 * contain the result of a join of different tables. For this reason a Cells object carries the
 * information of the table associated to each Cell. You may omit providing the table name
 * information, the provided Cell(s) object will be internally associated to a fictional default
 * table.
 * </p>
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public class Cells implements Iterable<Cell>, Serializable {
    private static final long serialVersionUID = 3074521612130550380L;

    /**
     * Internal default table name used when no table name is specified.
     */
    private String defaultTableName;

    private final static String DEFAULT_TABLE_NAME = "3fa2fbc6d8abbc77cdab9e3216d957dffd64a64b";

    /**
     * Maps a list of Cell to their table.
     */
    private Map<String, List<Cell>> cells = new HashMap<>();

    /**
     * Given the table name, returns the List of Cell object associated to that table.
     *
     * @param tableName the table name.
     * @return the List of Cell object associated to that table.
     */
    private List<Cell> getCellsByTable(String tableName) {
        String tName = StringUtils.isEmpty(tableName) ? defaultTableName : tableName;

        List<Cell> res = cells.get(tName);

        if (res == null) {
            res = new ArrayList<>();
            cells.put(tName, res);
        }

        return res;
    }

    /**
     * Constructs a new Cells object without a default table name.
     */
    public Cells() {
        this.defaultTableName = DEFAULT_TABLE_NAME;
    }

    /**
     * Builds a new Cells object using the provided table name as the default table name.
     */
    public Cells(String defaultTableName) {
        this.defaultTableName = defaultTableName;
    }

    /**
     * Builds a new Cells object containing the provided cells belonging to the default table.
     *
     * @param cells the array of Cells we want to use to create the Cells object.
     */
    public Cells(Cell... cells) {
        this(DEFAULT_TABLE_NAME, cells);
    }

    /**
     * Builds a new Cells object containing the provided cells belonging to <i>table</i>. Sets the
     * provided table name as the default table.
     *
     * @param cells the array of Cells we want to use to create the Cells object.
     */
    public Cells(String defaultTableName, Cell... cells) {
        this.defaultTableName = defaultTableName;
        if (StringUtils.isEmpty(defaultTableName)) {
            throw new IllegalArgumentException("table name cannot be null");
        }
        Collections.addAll(getCellsByTable(defaultTableName), cells);
    }

    /**
     * Adds a new Cell object to this Cells instance. Associates the provided Cell to the default
     * table.
     *
     * @param c the Cell we want to add to this Cells object.
     * @return either true/false if the Cell has been added successfully or not.
     */
    public boolean add(Cell c) {
        if (c == null) {
            throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
        }

        return getCellsByTable(defaultTableName).add(c);
    }

    /**
     * Adds a new Cell object to this Cells instance. Associates the provided Cell to the table whose
     * name is <i>table</i>.
     *
     * @param c the Cell we want to add to this Cells object.
     * @return either true/false if the Cell has been added successfully or not.
     */
    public boolean add(String table, Cell c) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("table name cannot be null");
        }

        if (c == null) {
            throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
        }

        return getCellsByTable(table).add(c);
    }

    /**
     * Adds a map of tables and cells to the current cells map. If the provided Cell already exists
     * for the table, it will be overridden.
     *
     * @param cells The cells map by table to be added to the current cells collection.
     */
    public void addAll(Map<String, List<Cell>> cells) {

        this.cells.putAll(cells);
    }

    /**
     * Replaces the cell (belonging to <i>table</i>) having the same name that the given one with the
     * given Cell object.
     *
     * @param c the Cell to replace the one in the Cells object.
     * @return either true/false if the Cell has been successfully replace or not.
     */
    public boolean replaceByName(String table, Cell c) {
        if (c == null) {
            throw new DeepGenericException(new IllegalArgumentException("cell parameter cannot be null"));
        }

        boolean cellFound = false;
        int position = 0;

        List<Cell> localCells = getCellsByTable(table);

        Iterator<Cell> cellsIt = localCells.iterator();
        while (!cellFound && cellsIt.hasNext()) {
            Cell currentCell = cellsIt.next();

            if (currentCell.getCellName().equals(c.getCellName())) {
                cellFound = true;
            } else {
                position++;
            }
        }

        if (cellFound) {
            localCells.remove(position);

            return localCells.add(c);
        }

        return false;
    }

    /**
     * Replaces the cell (belonging to the default table) having the same name that the given one with
     * the given Cell object.
     *
     * @param c the Cell to replace the one in the Cells object.
     * @return either true/false if the Cell has been successfully replace or not.
     */
    public boolean replaceByName(Cell c) {
        return replaceByName(defaultTableName, c);
    }

    /**
     * Removes the cell (belonging to <i>table</i>) with the given cell name.
     *
     * @param cellName the name of the cell to be removed.
     * @return either true/false if the Cell has been successfully removed or not.
     */
    public boolean remove(String table, String cellName) {

        if (cellName == null) {
            throw new DeepGenericException(new IllegalArgumentException(
                    "cell name parameter cannot be null"));
        }

        List<Cell> localCells = getCellsByTable(table);

        for (Cell currentCell : localCells) {
            if (currentCell.getCellName().equals(cellName)) {
                return localCells.remove(currentCell);
            }
        }

        return false;
    }

    /**
     * Removes the cell (belonging to the default table) with the given cell name.
     *
     * @param cellName the name of the cell to be removed.
     * @return either true/false if the Cell has been successfully removed or not.
     */
    public boolean remove(String cellName) {
        return remove(defaultTableName, cellName);
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

        if (this.size() != o.size()) {
            return false;
        }

        for (Map.Entry<String, List<Cell>> entry : cells.entrySet()) {
            List<Cell> localCells = entry.getValue();

            for (Cell cell : localCells) {
                Cell otherCell = o.getCellByName(entry.getKey(), cell.getCellName());

                if (otherCell == null || !otherCell.equals(cell)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Returns the cell at position idx in the list of Cell object associated to the default table.
     *
     * @param idx the index position of the Cell we want to retrieve.
     * @return Returns the cell at position idx.
     */
    public Cell getCellByIdx(int idx) {
        return getCellsByTable(defaultTableName).get(idx);
    }

    /**
     * Returns the cell at position idx in the list of Cell object associated to <i>table</i>.
     *
     * @param idx the index position of the Cell we want to retrieve.
     * @return Returns the cell at position idx.
     */
    public Cell getCellByIdx(String table, int idx) {
        return getCellsByTable(table).get(idx);
    }

    /**
     * Returns the Cell (associated to the default table) whose name is <i>cellName</i>, or null if
     * this Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the Cell whose name is cellName contained in this Cells object. null if no cell named
     * cellName is present.
     */
    public Cell getCellByName(String cellName) {
        return getCellByName(defaultTableName, cellName);
    }

    /**
     * Returns the Cell (associated to <i>table</i>) whose name is cellName, or null if this Cells
     * object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the Cell whose name is cellName contained in this Cells object. null if no cell named
     * cellName is present.
     */
    public Cell getCellByName(String table, String cellName) {

        for (Cell c : getCellsByTable(table)) {
            if (c.getCellName().equals(cellName)) {
                return c;
            }
        }
        return null;
    }

    /**
     * Returns an immutable collection of all the Cell objects contained in this Cells. Beware that
     * internally each list of cells is associated to the table owning those cells, this method
     * flattens the lists of cells known to this object to just one list, thus losing the table
     * information.
     *
     * @return the request list of Cell objects.
     */
    public Collection<Cell> getCells() {
        List<Cell> res = new ArrayList<>();

        for (Map.Entry<String, List<Cell>> entry : cells.entrySet()) {
            res.addAll(entry.getValue());
        }

        return Collections.unmodifiableList(res);
    }

    /**
     * Returns an immutable list of Cell object (associated to <i>table</i>) contained in this Cells.
     *
     * @param tableName the name of the owning table.
     * @return the requested list of Cell objects.
     */
    public Collection<Cell> getCells(String tableName) {
        return Collections.unmodifiableList(getCellsByTable(tableName));
    }

    /**
     * @return an immutable map mirroring the internal representation of this Cells object.
     */
    public Map<String, List<Cell>> getInternalCells() {
        return Collections.unmodifiableMap(cells);
    }

    /**
     * Converts every Cell (associated to the default table) contained in this object to an
     * ArrayBuffer. In order to perform the conversion we use the appropriate Cassandra marshaller for
     * the Cell.
     *
     * @return a collection of Cell(s) values converted to byte buffers using the appropriate
     * marshaller.
     */
    public Collection<ByteBuffer> getDecomposedCellValues() {
        return getDecomposedCellValues(defaultTableName);
    }

    /**
     * Converts every Cell (associated to <i>table</i>) contained in this object to an ArrayBuffer. In order to perform the
     * conversion we use the appropriate Cassandra marshaller for the Cell.
     *
     * @return a collection of Cell(s) values converted to byte buffers using the appropriate
     * marshaller.
     */
    public Collection<ByteBuffer> getDecomposedCellValues(String table) {
        List<ByteBuffer> res = new ArrayList<>();

        for (Cell c : getCellsByTable(table)) {
            ByteBuffer bb = c.getDecomposedCellValue();

            if (bb != null) {
                res.add(bb);
            }

        }

        return res;
    }

    /**
     * Converts every Cell (associated to <i>table</i>) contained in this object to an ArrayBuffer. In order to perform the
     * conversion we use the appropriate Cassandra marshaller for the Cell.
     *
     * @return a collection of Cell(s) values.
     */
    public Collection<Object> getCellValues(String table) {
        List<Object> res = new ArrayList<>();

        for (Cell c : getCellsByTable(table)) {
            res.add(c.getCellValue());
        }
        return res;
    }

    /**
     * Converts every Cell (associated to the default table) to an ArrayBuffer. In order to perform the
     * conversion we use the appropriate Cassandra marshaller for the Cell.
     *
     * @return a collection of Cell(s) values.
     */
    public Collection<Object> getCellValues() {
        return getCellValues(defaultTableName);
    }

    /**
     * Extracts from this object the Cell(s) associated to <i>table</i> and marked either as partition key or cluster key.
     * Returns an empty Cells object if the current object does not contain any Cell marked as key.
     *
     * @return the Cells object containing the subset of this Cells object of only the Cell(s) part of
     * the key.
     */
    public Cells getIndexCells(String table) {
        Cells res = new Cells(table);
        for (Cell cell : getCellsByTable(table)) {
            if (cell.isKey()) {
                res.add(table, cell);
            }

        }

        return res;
    }

    /**
     * Extracts the Cell(s) associated to the default table and marked either as partition key or cluster key.
     * Returns an empty Cells object if the current object does not contain any Cell marked as key.
     *
     * @return the Cells object containing the subset of this Cells object of only the Cell(s) part of
     * the key.
     */
    public Cells getIndexCells() {
        Cells res = new Cells(this.defaultTableName);

        for (Map.Entry<String, List<Cell>> entry : cells.entrySet()) {
            Cells keys = getIndexCells(entry.getKey());

            for (Cell c : keys) {
                res.add(entry.getKey(), c);
            }
        }

        return res;
    }

    /**
     * Extracts the cells associated to <i>table</i> _NOT_ marked as partition key and _NOT_ marked as
     * cluster key b.
     *
     * @return the Cells object containing the subset of this Cells object of only the Cell(s) that
     * are NOT part of the key.
     */
    public Cells getValueCells(String table) {
        Cells res = new Cells(table);
        for (Cell cell : getCellsByTable(table)) {
            if (!cell.isKey()) {
                res.add(table, cell);
            }
        }

        return res;
    }

    /**
     * Extracts the Cell(s) associated to the default table _NOT_ marked as partition key and _NOT_
     * marked as cluster key.
     *
     * @return the Cells object containing the subset of this Cells object of only the Cell(s) that
     * are NOT part of the key.
     */
    public Cells getValueCells() {
        Cells res = new Cells(this.defaultTableName);

        for (Map.Entry<String, List<Cell>> entry : cells.entrySet()) {
            Cells keys = getValueCells(entry.getKey());

            for (Cell c : keys) {
                res.add(entry.getKey(), c);
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
     * Iterated over the full list of Cell object contained in this object, no matter to which table
     * each Cell is associated to.
     */
    @Override
    public Iterator<Cell> iterator() {
        return getCells().iterator();
    }

    /**
     * Returns the total number of cell(s) this object contains.
     *
     * @return the number os Cell objects contained in this Cells object.
     */
    public int size() {

        int acc = 0;

        for (Map.Entry<String, List<Cell>> entry : cells.entrySet()) {
            acc += entry.getValue().size();
        }

        return acc;
    }

    /**
     * Returns the total number of cells associated to <i>table</i> this object contains.
     *
     * @param table the table name
     * @return the total number of cells associated to <i>table</i> this object contains.
     */
    public int size(String table) {
        return getCellsByTable(table).size();
    }

    /**
     * @return true if this object contains no cells.
     */
    public boolean isEmpty() {
        if (cells.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, List<Cell>> entry : cells.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Cells{" + "cells=" + cells + '}';
    }

    /**
     * Default table name getter.
     *
     * @return the default table name.
     */
    public String getDefaultTableName() {
        return defaultTableName;
    }

    /**
     * Returns the casted value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @param cellClass the class of the cell's value
     * @param <T>       the type of the cell's value
     * @return the casted value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public <T> T getValue(String tableName, int idx, Class<T> cellClass) {
        Cell cell = getCellByIdx(tableName, idx);
        return cell == null ? null : cell.getValue(cellClass);
    }

    /**
     * Returns the casted value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object.
     *
     * @param idx       the index position of the Cell we want to retrieve
     * @param cellClass the class of the cell's value
     * @param <T>       the type of the cell's value
     * @return the casted value of the  {@link Cell} at position {@code idx} in the list of Cell object
     */
    public <T> T getValue(int idx, Class<T> cellClass) {
        Cell cell = getCellByIdx(idx);
        return cell == null ? null : cell.getValue(cellClass);
    }

    /**
     * Returns the casted value of the {@link Cell} (associated to {@code table}) whose name is
     * cellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @param cellClass the class of the cell's value
     * @param <T>       the type of the cell's value
     * @return the casted value of the {@link Cell} (associated to {@code table}) whose name is
     * cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public <T> T getValue(String tableName, String cellName, Class<T> cellClass) {
        Cell cell = getCellByName(tableName, cellName);
        return cell == null ? null : cell.getValue(cellClass);
    }

    /**
     * Returns the casted value of the {@link Cell} whose name is cellName, or null if this Cells
     * object contains no cell whose name is cellName.
     *
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @param cellClass the class of the cell's value
     * @param <T>       the type of the cell's value
     * @return the casted value of the {@link Cell} whose name is cellName, or null if this Cells
     * object contains no cell whose name is cellName
     */
    public <T> T getValue(String cellName, Class<T> cellClass) {
        Cell cell = getCellByName(cellName);
        return cell == null ? null : cell.getValue(cellClass);
    }


    public <T> List<T> getList(String tableName, int idx, Class<T> elementsClass) {
        Cell cell = getCellByIdx(tableName, idx);
        return cell == null ? null : cell.getList(elementsClass);
    }

    public <T> List<T> getList(int idx, Class<T> elementsClass) {
        Cell cell = getCellByIdx(idx);
        return cell == null ? null : cell.getList(elementsClass);
    }

    public <T> List<T> getList(String tableName, String cellName, Class<T> elementsClass) {
        Cell cell = getCellByName(tableName, cellName);
        return cell == null ? null : cell.getList(elementsClass);
    }

    public <T> List<T> getList(String cellName, Class<T> elementsClass) {
        Cell cell = getCellByName(cellName);
        return cell == null ? null : cell.getList(elementsClass);
    }


    public <T> Set<T> getSet(String tableName, int idx, Class<T> elementsClass) {
        Cell cell = getCellByIdx(tableName, idx);
        return cell == null ? null : cell.getSet(elementsClass);
    }

    public <T> Set<T> getSet(int idx, Class<T> elementsClass) {
        Cell cell = getCellByIdx(idx);
        return cell == null ? null : cell.getSet(elementsClass);
    }

    public <T> Set<T> getSet(String tableName, String cellName, Class<T> elementsClass) {
        Cell cell = getCellByName(tableName, cellName);
        return cell == null ? null : cell.getSet(elementsClass);
    }

    public <T> Set<T> getSet(String cellName, Class<T> elementsClass) {
        Cell cell = getCellByName(cellName);
        return cell == null ? null : cell.getSet(elementsClass);
    }


    public <K, V> Map<K, V> getMap(String tableName,
                                   int idx,
                                   Class<K> keysClass,
                                   Class<V> valuesClass) {
        Cell cell = getCellByIdx(tableName, idx);
        return cell == null ? null : cell.getMap(keysClass, valuesClass);
    }

    public <K, V> Map<K, V> getMap(int idx, Class<K> keysClass, Class<V> valuesClass) {
        Cell cell = getCellByIdx(idx);
        return cell == null ? null : cell.getMap(keysClass, valuesClass);
    }

    public <K, V> Map<K, V> getMap(String tableName,
                                   String cellName,
                                   Class<K> keysClass,
                                   Class<V> valuesClass) {
        Cell cell = getCellByName(tableName, cellName);
        return cell == null ? null : cell.getMap(keysClass, valuesClass);
    }

    public <K, V> Map<K, V> getMap(String cellName, Class<K> keysClass, Class<V> valuesClass) {
        Cell cell = getCellByName(cellName);
        return cell == null ? null : cell.getMap(keysClass, valuesClass);
    }


    /**
     * Returns the {@code String} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code String} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public String getString(String tableName, int idx) {
        return getValue(tableName, String.class);
    }

    /**
     * Returns the {@code String} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code String} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public String getString(int idx) {
        return getValue(idx, String.class);
    }

    /**
     * Returns the {@code String} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code String} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public String getString(String tableName, String cellName) {
        return getValue(tableName, cellName, String.class);
    }

    /**
     * Returns the {@code String} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code String} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public String getString(String cellName) {
        return getValue(cellName, String.class);
    }


    /**
     * Returns the {@code Character} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Character} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Character getCharacter(String tableName, int idx) {
        return getValue(tableName, Character.class);
    }

    /**
     * Returns the {@code Character} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Character} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Character getCharacter(int idx) {
        return getValue(idx, Character.class);
    }

    /**
     * Returns the {@code Character} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Character} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Character getCharacter(String tableName, String cellName) {
        return getValue(tableName, cellName, Character.class);
    }

    /**
     * Returns the {@code Character} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Character} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Character getCharacter(String cellName) {
        return getValue(cellName, Character.class);
    }

    /**
     * Returns the {@code Byte} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Byte} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Byte getByte(String tableName, int idx) {
        return getValue(tableName, Byte.class);
    }

    /**
     * Returns the {@code Byte} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Byte} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Byte getByte(int idx) {
        return getValue(idx, Byte.class);
    }

    /**
     * Returns the {@code Byte} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Byte} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Byte getByte(String tableName, String cellName) {
        return getValue(tableName, cellName, Byte.class);
    }

    /**
     * Returns the {@code Byte} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Byte} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Byte getByte(String cellName) {
        return getValue(cellName, Byte.class);
    }

    /**
     * Returns the {@code Date} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Date} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Date getDate(String tableName, int idx) {
        return getValue(tableName, Date.class);
    }

    /**
     * Returns the {@code Date} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Date} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Date getDate(int idx) {
        return getValue(idx, Date.class);
    }

    /**
     * Returns the {@code Date} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Date} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Date getDate(String tableName, String cellName) {
        return getValue(tableName, cellName, Date.class);
    }

    /**
     * Returns the {@code Date} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the Date of the Cell we want to retrieve from this Cells object.
     * @return the {@code String} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Date getDate(String cellName) {
        return getValue(cellName, Date.class);
    }

    /**
     * Returns the {@code Boolean} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Boolean} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Boolean getBoolean(String tableName, int idx) {
        return getValue(tableName, Boolean.class);
    }

    /**
     * Returns the {@code Boolean} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Boolean} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Boolean getBoolean(int idx) {
        return getValue(idx, Boolean.class);
    }

    /**
     * Returns the {@code Boolean} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Boolean} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Boolean getBoolean(String tableName, String cellName) {
        return getValue(tableName, cellName, Boolean.class);
    }

    /**
     * Returns the {@code Boolean} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the Date of the Cell we want to retrieve from this Cells object.
     * @return the {@code Boolean} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Boolean getBoolean(String cellName) {
        return getValue(cellName, Boolean.class);
    }

    /**
     * Returns the {@code Short} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Short} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Short getShort(String tableName, int idx) {
        return getValue(tableName, Short.class);
    }

    /**
     * Returns the {@code Short} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Short} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Short getShort(int idx) {
        return getValue(idx, Short.class);
    }

    /**
     * Returns the {@code Short} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Short} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Short getShort(String tableName, String cellName) {
        return getValue(tableName, cellName, Short.class);
    }

    /**
     * Returns the {@code Short} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Short} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Short getShort(String cellName) {
        return getValue(cellName, Short.class);
    }

    /**
     * Returns the {@code Integer} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Integer} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Integer getInteger(String tableName, int idx) {
        return getValue(tableName, Integer.class);
    }

    /**
     * Returns the {@code Integer} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Integer} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Integer getInteger(int idx) {
        return getValue(idx, Integer.class);
    }

    /**
     * Returns the {@code Integer} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Integer} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Integer getInteger(String tableName, String cellName) {
        return getValue(tableName, cellName, Integer.class);
    }

    /**
     * Returns the {@code Integer} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Integer} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Integer getInteger(String cellName) {
        return getValue(cellName, Integer.class);
    }

    /**
     * Returns the {@code Float} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Float} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Float getFloat(String tableName, int idx) {
        return getValue(tableName, Float.class);
    }

    /**
     * Returns the {@code Float} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Float} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Float getFloat(int idx) {
        return getValue(idx, Float.class);
    }

    /**
     * Returns the {@code Float} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Float} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Float getFloat(String tableName, String cellName) {
        return getValue(tableName, cellName, Float.class);
    }

    /**
     * Returns the {@code Float} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Float} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Float getFloat(String cellName) {
        return getValue(cellName, Float.class);
    }

    /**
     * Returns the {@code Long} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Long} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Long getLong(String tableName, int idx) {
        return getValue(tableName, Long.class);
    }

    /**
     * Returns the {@code Long} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Long} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Long getLong(int idx) {
        return getValue(idx, Long.class);
    }

    /**
     * Returns the {@code Long} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Long} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Long getLong(String tableName, String cellName) {
        return getValue(tableName, cellName, Long.class);
    }

    /**
     * Returns the {@code Long} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Long} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Long getLong(String cellName) {
        return getValue(cellName, Long.class);
    }

    /**
     * Returns the {@code Double} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Double} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Double getDouble(String tableName, int idx) {
        return getValue(tableName, Double.class);
    }

    /**
     * Returns the {@code Double} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Double} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Double getDouble(int idx) {
        return getValue(idx, Double.class);
    }

    /**
     * Returns the {@code Double} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Double} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Double getDouble(String tableName, String cellName) {
        return getValue(tableName, cellName, Double.class);
    }

    /**
     * Returns the {@code Double} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Double} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Double getDouble(String cellName) {
        return getValue(cellName, Double.class);
    }

    /**
     * Returns the {@code BigInteger} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code BigInteger} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public BigInteger getBigInteger(String tableName, int idx) {
        return getValue(tableName, BigInteger.class);
    }

    /**
     * Returns the {@code BigInteger} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code BigInteger} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public BigInteger getBigInteger(int idx) {
        return getValue(idx, BigInteger.class);
    }

    /**
     * Returns the {@code BigInteger} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code BigInteger} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public BigInteger getBigInteger(String tableName, String cellName) {
        return getValue(tableName, cellName, BigInteger.class);
    }

    /**
     * Returns the {@code BigInteger} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code BigInteger} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public BigInteger getBigInteger(String cellName) {
        return getValue(cellName, BigInteger.class);
    }

    /**
     * Returns the {@code BigDecimal} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code BigDecimal} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public BigDecimal getBigDecimal(String tableName, int idx) {
        return getValue(tableName, BigDecimal.class);
    }

    /**
     * Returns the {@code BigDecimal} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code BigDecimal} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public BigDecimal getBigDecimal(int idx) {
        return getValue(idx, BigDecimal.class);
    }

    /**
     * Returns the {@code BigInteger} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code BigInteger} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public BigDecimal getBigDecimal(String tableName, String cellName) {
        return getValue(tableName, cellName, BigDecimal.class);
    }

    /**
     * Returns the {@code BigInteger} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code BigInteger} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public BigDecimal getBigDecimal(String cellName) {
        return getValue(cellName, BigDecimal.class);
    }


    /**
     * Returns the {@code ByteBuffer} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code ByteBuffer} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public ByteBuffer getByteBuffer(String tableName, int idx) {
        return getValue(tableName, ByteBuffer.class);
    }

    /**
     * Returns the {@code ByteBuffer} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code ByteBuffer} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public ByteBuffer getByteBuffer(int idx) {
        return getValue(idx, ByteBuffer.class);
    }

    /**
     * Returns the {@code ByteBuffer} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code ByteBuffer} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public ByteBuffer getByteBuffer(String tableName, String cellName) {
        return getValue(tableName, cellName, ByteBuffer.class);
    }

    /**
     * Returns the {@code ByteBuffer} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code ByteBuffer} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public ByteBuffer getByteBuffer(String cellName) {
        return getValue(cellName, ByteBuffer.class);
    }


    /**
     * Returns the {@code URL} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code URL} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public URL getURL(String tableName, int idx) {
        return getValue(tableName, URL.class);
    }

    /**
     * Returns the {@code URL} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code URL} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public URL getURL(int idx) {
        return getValue(idx, URL.class);
    }

    /**
     * Returns the {@code URL} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code URL} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public URL getURL(String tableName, String cellName) {
        return getValue(tableName, cellName, URL.class);
    }

    /**
     * Returns the {@code URL} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code URL} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public URL getURL(String cellName) {
        return getValue(cellName, URL.class);
    }


    /**
     * Returns the {@code InetAddress} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code InetAddress} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public InetAddress getInetAddress(String tableName, int idx) {
        return getValue(tableName, InetAddress.class);
    }

    /**
     * Returns the {@code InetAddress} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code InetAddress} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public InetAddress getInetAddress(int idx) {
        return getValue(idx, InetAddress.class);
    }

    /**
     * Returns the {@code InetAddress} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code InetAddress} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public InetAddress getInetAddress(String tableName, String cellName) {
        return getValue(tableName, cellName, InetAddress.class);
    }

    /**
     * Returns the {@code InetAddress} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code InetAddress} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public InetAddress getInetAddress(String cellName) {
        return getValue(cellName, InetAddress.class);
    }

    /**
     * Returns the {@code Byte[]} value of the {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}.
     *
     * @param tableName the name of the owning table
     * @param idx       the index position of the Cell we want to retrieve
     * @return the {@code Byte[]} value of the  {@link Cell} at position {@code idx} in the list of Cell
     * object associated to {@code table}
     */
    public Byte[] getBytes(String tableName, int idx) {
        return getValue(tableName, Byte[].class);
    }

    /**
     * Returns the {@code Byte[]} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object.
     *
     * @param idx the index position of the Cell we want to retrieve
     * @return the {@code Byte[]} value of the  {@link Cell} at position {@code idx} in the list of
     * Cell object
     */
    public Byte[] getBytes(int idx) {
        return getValue(idx, Byte[].class);
    }

    /**
     * Returns the {@code Byte[]} value of the {@link Cell} (associated to {@code table}) whose name
     * iscellName, or null if this Cells object contains no cell whose name is cellName.
     *
     * @param tableName the name of the owning table
     * @param cellName  the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Byte[]} value of the {@link Cell} (associated to {@code table}) whose name
     * is cellName, or null if this Cells object contains no cell whose name is cellName
     */
    public Byte[] getBytes(String tableName, String cellName) {
        return getValue(tableName, cellName, Byte[].class);
    }

    /**
     * Returns the {@code Byte[]} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName.
     *
     * @param cellName the name of the Cell we want to retrieve from this Cells object.
     * @return the {@code Byte[]} value of the {@link Cell} whose name is cellName, or null if this
     * Cells object contains no cell whose name is cellName
     */
    public Byte[] getBytes(String cellName) {
        return getValue(cellName, Byte[].class);
    }

}
