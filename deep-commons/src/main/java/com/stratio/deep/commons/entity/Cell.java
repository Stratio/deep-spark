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

import com.datastax.driver.core.ResultSet;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by rcrespo on 2/07/14.
 */
public  class Cell implements Serializable {

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

    public static Cell create(String cellName, Object cellValue) {

        return new Cell(cellName, cellValue);
    }

    public String getCellName() {
        return this.cellName;
    }

    public Object getCellValue() {
        return this.cellValue;
    }

    /**
     * Returns the cell name.
     *
     * @return the cell name.
     */
    public String getName() {
        return this.cellName;
    }

    public Object getValue() {
        return this.cellValue;
    }

    /**
     * Returns the cell value casted to the specified class.
     *
     * @param clazz the expected class
     * @param <T>   the return type
     * @return the cell value casted to the specified class
     */
    public <T> T getValue(Class<T> clazz) {
        if (this.cellValue == null) {
            return null;
        } else {
            return (T) this.cellValue;
        }
    }

    /**
     * Returns the cell value casted as a {@code String}.
     *
     * @return the cell value casted as a {@code String}.
     */
    public String getString() {
        return getValue(String.class);
    }

    /**
     * Returns the cell value casted as a {@code Boolean}.
     *
     * @return the cell value casted as a {@code Boolean}.
     */
    public Boolean getBoolean() {
        return getValue(Boolean.class);
    }

    /**
     * Returns the cell value casted as a {@code Date}.
     *
     * @return the cell value casted as a {@code Date}.
     */
    public Date getDate() {
        return getValue(Date.class);
    }

    /**
     * Returns the cell value casted as a {@code UUID}.
     *
     * @return the cell value casted as a {@code UUID}.
     */
    public UUID getUUID() {
        return getValue(UUID.class);
    }

    /**
     * Returns the cell value casted as a {@code Short}.
     *
     * @return the cell value casted as a {@code Short}.
     */
    public Short getShort() {
        return getValue(Short.class);
    }

    /**
     * Returns the cell value casted as a {@code Byte}.
     *
     * @return the cell value casted as a {@code Byte}.
     */
    public Byte getByte() {
        return getValue(Byte.class);
    }

    /**
     * Returns the cell value casted as a {@code Byte[]}.
     *
     * @return the cell value casted as a {@code Byte[]}.
     */
    public Byte[] getBytes() {
        return getValue(Byte[].class);
    }

    /**
     * Returns the cell value casted as a {@code Character}.
     *
     * @return the cell value casted as a {@code Character}.
     */
    public Character getCharacter() {
        return getValue(Character.class);
    }

    /**
     * Returns the cell value casted as a {@code Integer}.
     *
     * @return the cell value casted as a {@code Integer}.
     */
    public Integer getInteger() {
        return getValue(Integer.class);
    }

    /**
     * Returns the cell value casted as a {@code Long}.
     *
     * @return the cell value casted as a {@code Long}.
     */
    public Long getLong() {
        return getValue(Long.class);
    }

    /**
     * Returns the cell value casted as a {@code BigInteger}.
     *
     * @return the cell value casted as a {@code BigInteger}.
     */
    public BigInteger getBigInteger() {
        return getValue(BigInteger.class);
    }

    /**
     * Returns the cell value casted as a {@code Float}.
     *
     * @return the cell value casted as a {@code Float}.
     */
    public Float getFloat() {
        return getValue(Float.class);
    }

    /**
     * Returns the cell value casted as a {@code Double}.
     *
     * @return the cell value casted as a {@code Double}.
     */
    public Double getDouble() {
        return getValue(Double.class);
    }

    /**
     * Returns the cell value casted as a {@code BigDecimal}.
     *
     * @return the cell value casted as a {@code BigDecimal}.
     */
    public BigDecimal getBigDecimal() {
        return getValue(BigDecimal.class);
    }

    /**
     * Returns the cell value casted as a {@code URL}.
     *
     * @return the cell value casted as a {@code URL}.
     */
    public URL getURL() {
        return getValue(URL.class);
    }

    /**
     * Returns the cell value casted as a {@code InetAddress}.
     *
     * @return the cell value casted as a {@code InetAddress}.
     */
    public InetAddress getInetAddress() {
        return getValue(InetAddress.class);
    }

    public <T> List<T> getList(Class<T> clazz) {
        if (this.cellValue == null) {
            return null;
        } else {
            return (List<T>) this.cellValue;
        }
    }

    public <T> Set<T> getSet(Class<T> clazz) {
        if (this.cellValue == null) {
            return null;
        } else {
            return (Set<T>) this.cellValue;
        }
    }

    public <K, V> Map<K, V> getMap(Class<K> keysClass, Class<V> valuesClass) {
        if (this.cellValue == null) {
            return null;
        } else {
            return (Map<K, V>) this.cellValue;
        }
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

        return this.cellName.equals(cell.cellName)&&this.cellValue.equals(cell.cellValue);
    }

    /**
     * @return true is the current cell is a key inside the datastore, false
     *         otherwise.
     */
    public Boolean isKey(){
        return false;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Cell{");
        sb.append("cellName='").append(this.cellName).append('\'');
        sb.append(", cellValue=").append(this.cellValue);
        sb.append('}');
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public ByteBuffer getDecomposedCellValue() {
        return null;
    }

}
