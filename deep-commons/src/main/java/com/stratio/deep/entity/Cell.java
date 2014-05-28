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

import com.datastax.driver.core.DataType;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepInstantiationException;
import org.apache.cassandra.db.marshal.AbstractType;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static com.stratio.deep.utils.AnnotationUtils.*;

/**
 * Generic abstraction for cassandra's columns.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class Cell implements Serializable {

    private static final long serialVersionUID = 2298549804049316156L;

    /**
     * Name of the cell. Mapped to a Cassandra column name.
     */
    private String cellName;

    /**
     * Cell value.
     */
    private Object cellValue;

    /**
     * flag that tells if this cell is part of the partition key.
     * Defaults to FALSE.
     */
    private Boolean isPartitionKey = Boolean.FALSE;

    /**
     * flag that tells if this cell is part of the clustering key.
     * Defaults to FALSE.
     */
    private Boolean isClusterKey = Boolean.FALSE;

    private CellValidator cellValidator;

    /**
     * Factory method, creates a new Cell from its value and metadata information<br/>
     *
     * @param metadata  the cell object carrying the metadata for this new cell
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(Cell metadata, Object cellValue) {
        return new Cell(metadata, cellValue);
    }

    /**
     * Factory method, creates a new Cell from its value and metadata information<br/>
     *
     * @param metadata  the cell object carrying the metadata for this new cell
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(Cell metadata, ByteBuffer cellValue) {
        return new Cell(metadata, cellValue);
    }

    /**
     * Factory method, builds a new Cell (isPartitionKey = false and isClusterKey = false).
     * The validator will be automatically calculated using the value object type.
     *
     * @param cellName  the cell name
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(String cellName, Object cellValue) {
        return create(cellName, cellValue, Boolean.FALSE, Boolean.FALSE);
    }

    /**
     * Factory method, builds a new Cell (isPartitionKey = false and isClusterKey = false) with value = null.
     * The validator will be automatically calculated using the value object type.
     *
     * @param cellName  the cell name
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(String cellName) {
        return create(cellName, (Object)null, Boolean.FALSE, Boolean.FALSE);
    }

    /**
     * Factory method, creates a new Cell.<br/>
     *
     * @param cellName       the cell name
     * @param cellValue      the cell value, provided as a ByteBuffer.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey   true if this cell is part of the cassandra's clustering key.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(String cellName, Object cellValue, Boolean isPartitionKey,
                              Boolean isClusterKey) {
        return new Cell(cellName, cellValue, isPartitionKey, isClusterKey);
    }

    /**
     * Factory method, creates a new metadata Cell, i.e. a Cell without value.
     *
     * @param cellName       the cell name
     * @param cellType       the cell value type.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey   true if this cell is part of the cassandra's clustering key.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(String cellName, DataType cellType, Boolean isPartitionKey,
                              Boolean isClusterKey) {
        return new Cell(cellName, cellType, isPartitionKey, isClusterKey);
    }

    /**
     * Constructs a Cell from a {@link com.stratio.deep.annotations.DeepField} property.
     *
     * @param e     instance of the testentity whose field is going to generate a Cell.
     * @param field field that will generate the Cell.
     * @param <E>   a subclass of IDeepType.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static <E extends IDeepType> Cell create(E e, Field field) {
        return new Cell(e, field);
    }

    /**
     * Calculates the Cassandra validator type given the value class type.<br/>
     * There's a shortcoming in the case of an UUID. At this level we are not able
     * to distinguish between an UUID or a TimeUUID because twe don't have the UUID value.
     *
     * @param obj the value class type.
     * @return the CellValidator object associated to this cell.
     */
    public static CellValidator getValueType(DataType obj) {
        return CellValidator.cellValidator(obj);
    }

    /**
     * Calculates the Cassandra marshaller given the cell value.
     *
     * @param obj the cell value.
     * @return the CellValidator object associated to this cell.
     */
    public static CellValidator getValueType(Object obj) {
        return CellValidator.cellValidator(obj);
    }

    /**
     * Private constructor.
     */
    private Cell(String cellName, Object cellValue, Boolean isPartitionKey, Boolean isClusterKey) {
        if (cellValue != null && !(cellValue instanceof Serializable)) {
            throw new DeepInstantiationException("provided cell value " + cellValue + " is not serializable");
        }
        this.cellName = cellName;
        this.cellValue = cellValue;
        this.isClusterKey = isClusterKey;
        this.isPartitionKey = isPartitionKey;
        this.cellValidator = getValueType(cellValue);
    }

    /**
     * Private constructor.
     */
    private Cell(String cellName, DataType cellType, Boolean isPartitionKey, Boolean isClusterKey) {
        this.cellName = cellName;
        this.isClusterKey = isClusterKey;
        this.isPartitionKey = isPartitionKey;
        this.cellValidator = getValueType(cellType);
    }

    /**
     * Private constructor.
     */
    private Cell(Cell metadata, ByteBuffer cellValue) {
        this.cellName = metadata.getCellName();
        this.isClusterKey = metadata.isClusterKey;
        this.isPartitionKey = metadata.isPartitionKey;
        this.cellValidator = metadata.cellValidator;
        if (cellValue != null) {
            this.cellValue = metadata.marshaller().compose(cellValue);
        }
    }

    /**
     * Private constructor.
     */
    private Cell(Cell metadata, Object cellValue) {
        if (!(cellValue instanceof Serializable)) {
            throw new DeepInstantiationException("provided cell value " + cellValue + " is not serializable");
        }

        this.cellName = metadata.getCellName();
        this.isClusterKey = metadata.isClusterKey;
        this.isPartitionKey = metadata.isPartitionKey;
        this.cellValidator = metadata.cellValidator;
        this.cellValue = cellValue;
    }

    /**
     * Private constructor.
     */
    private Cell(IDeepType e, Field field) {

        DeepField annotation = field.getAnnotation(DeepField.class);
        this.cellName = deepFieldName(field);
        this.cellValue = getBeanFieldValue(e, field);
        this.isClusterKey = annotation.isPartOfClusterKey();
        this.isPartitionKey = annotation.isPartOfPartitionKey();
        this.cellValidator = CellValidator.cellValidator(field);
    }

    /**
     * @return Returns the validator object associated to this Cell.
     */
    public CellValidator getCellValidator() {
        return cellValidator;
    }

    /**
     * Two Cell are considered equal if and only if the following properties are equal:
     * <ol>
     * <li>cellName</li>
     * <li>cellValue</li>
     * <li>isClusterKey</li>
     * <li>isPartitionKey</li>
     * <li>validator</li>
     * </ol>
     *
     * @param o the object instance we want to compare to this object.
     * @return either true or false if this Cell is equal to the one supplied as an argument.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Cell cell = (Cell) o;

        return cellName.equals(cell.cellName) &&
                (cellValue != null ? cellValue.equals(cell.cellValue) : cell.cellValue != null) &&
                isClusterKey.equals(cell.isClusterKey) &&
                isPartitionKey.equals(cell.isPartitionKey) &&
                cellValidator.equals(cell.getCellValidator());
    }

    /**
     * Returns the Java type corresponding to the current Cell.
     *
     * @return the Java type corresponding to the current Cell.
     */
    public Class<?> getValueType() {
        Class valueType = MAP_ABSTRACT_TYPE_CLASSNAME_TO_JAVA_TYPE.get(cellValidator.getValidatorClassName());
        if (valueType == null) {
            throw new DeepGenericException("Cannot find value type for marshaller " + cellValidator
                    .getValidatorClassName());
        }

        return valueType;
    }

    /**
     * @return the name of the current cell as defined in the database.
     */
    public String getCellName() {
        return cellName;
    }

    /**
     * Returns the composed cell value. The type of the returned object can be obtained by calling
     * {@link com.stratio.deep.entity.Cell#getValueType}
     *
     * @return the composed cell value.
     */
    public Object getCellValue() {
        return cellValue;
    }

    /**
     * Returns the cell value as a ByteBuffer, performs the conversion using the
     * configured validator.
     * <p/>
     * If cell value is null we propagate an empty array, see CASSANDRA-5885 and CASSANDRA-6180.
     *
     * @return Returns the cell value as a ByteBuffer.
     */
    @SuppressWarnings("unchecked")
    public ByteBuffer getDecomposedCellValue() {

        if (this.cellValue != null) {
            return marshaller().decompose(this.cellValue);
        } else {
            /* if null we propagate an empty array, see CASSANDRA-5885 and CASSANDRA-6180 */
            return ByteBuffer.wrap(new byte[0]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = cellName.hashCode();
        result = 31 * result + (cellValue != null ? cellValue.hashCode() : 0);
        result = 31 * result + isPartitionKey.hashCode();
        result = 31 * result + isClusterKey.hashCode();
        result = 31 * result + cellValidator.hashCode();
        return result;
    }

    /**
     * @return true if the current cell is part of the clustering key.
     */
    public Boolean isClusterKey() {
        return isClusterKey;
    }

    /**
     * @return true if the current cell is part of the partition key.
     */
    public Boolean isPartitionKey() {
        return isPartitionKey;
    }

    /**
     * @return Returns the Cassandra validator instance associated to this cell.
     */
    public AbstractType marshaller() {
        if (cellValidator != null) {
            return cellValidator.getAbstractType();
        } else {
            return null;
        }
    }

    /**
     * @return the fully qualified class name of the cassandra validator associated to this cell.
     */
    public String marshallerClassName() {
        if (cellValidator != null) {
            return cellValidator.getValidatorClassName();
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Cell{" + "cellName='" + cellName + '\'' + ", cellValue=" + (cellValue != null ? cellValue : "") + ", " +
                "isPartitionKey="
                + isPartitionKey + ", isClusterKey=" + isClusterKey + ", cellValidator='" + cellValidator + '\'' + '}';
    }
}
