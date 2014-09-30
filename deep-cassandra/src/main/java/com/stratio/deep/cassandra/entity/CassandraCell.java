package com.stratio.deep.cassandra.entity;

import com.datastax.driver.core.DataType;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepInstantiationException;
import org.apache.cassandra.db.marshal.AbstractType;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static com.stratio.deep.commons.utils.AnnotationUtils.*;


/**
 * Created by rcrespo on 23/06/14.
 */
public class CassandraCell extends Cell {

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

    private transient CellValidator cellValidator;


    private String cql3TypeClassName;
    /**
     * Factory method, creates a new CassandraCell from its value and metadata information<br/>
     *
     * @param metadata  the cell object carrying the metadata for this new CassandraCell
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(Cell metadata, Object cellValue) {
        return new CassandraCell(metadata, cellValue);
    }

    /**
     * Factory method, creates a new CassandraCell from its value and metadata information<br/>
     *
     * @param metadata  the cell object carrying the metadata for this new CassandraCell
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(Cell metadata, ByteBuffer cellValue) {
        return new CassandraCell(metadata, cellValue);
    }

    /**
     * Factory method, builds a new CassandraCell (isPartitionKey = false and isClusterKey = false).
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
     * Factory method, builds a new CassandraCell (isPartitionKey = false and isClusterKey = false) with value = null.
     * The validator will be automatically calculated using the value object type.
     *
     * @param cellName the cell name
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(String cellName) {
        return create(cellName, (Object) null, Boolean.FALSE, Boolean.FALSE);
    }

    /**
     * Factory method, creates a new CassandraCell.<br/>
     *
     * @param cellName       the cell name
     * @param cellValue      the cell value, provided as a ByteBuffer.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey   true if this cell is part of the cassandra's clustering key.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(String cellName, Object cellValue, Boolean isPartitionKey,
                              Boolean isClusterKey) {
        return new CassandraCell(cellName, cellValue, isPartitionKey, isClusterKey);
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
        return new CassandraCell(cellName, cellType, isPartitionKey, isClusterKey);
    }

    /**
     * Constructs a Cell from a {@link com.stratio.deep.commons.annotations.DeepField} property.
     *
     * @param e     instance of the testentity whose field is going to generate a Cell.
     * @param field field that will generate the Cell.
     * @param <E>   a subclass of IDeepType.
     * @return an instance of a Cell object for the provided parameters.
     */
    public static <E extends IDeepType> Cell create(E e, Field field) {
        return new CassandraCell(e, field);
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
    private CassandraCell(String cellName, Object cellValue, Boolean isPartitionKey, Boolean isClusterKey) {
        if (cellValue != null && !(cellValue instanceof Serializable)) {
            throw new DeepInstantiationException("provided cell value " + cellValue + " is not serializable");
        }
        this.cellName = cellName;
        this.cellValue = cellValue;
        this.isClusterKey = isClusterKey;
        this.isPartitionKey = isPartitionKey;
        this.cellValidator = getValueType(cellValue);
        this.cql3TypeClassName = cellValidator!=null?cellValidator.getAbstractType().asCQL3Type().toString():null;

    }

    /**
     * Private constructor.
     */
    private CassandraCell(String cellName, DataType cellType, Boolean isPartitionKey, Boolean isClusterKey) {
        this.cellName = cellName;
        this.isClusterKey = isClusterKey;
        this.isPartitionKey = isPartitionKey;
        this.cellValidator = getValueType(cellType);
        this.cql3TypeClassName = cellValidator.getAbstractType().asCQL3Type().toString();

    }

    /**
     * Private constructor.
     */
    private CassandraCell(Cell metadata, ByteBuffer cellValue) {
        this.cellName = metadata.getCellName();
        this.isClusterKey = ((CassandraCell) metadata).isClusterKey;
        this.isPartitionKey = ((CassandraCell) metadata).isPartitionKey;
        this.cellValidator = ((CassandraCell) metadata).cellValidator;
        if (cellValue != null) {
            this.cellValue = ((CassandraCell) metadata).marshaller().compose(cellValue);
        }
        this.cql3TypeClassName = cellValidator.getAbstractType().asCQL3Type().toString();

    }

    /**
     * Private constructor.
     */
    private CassandraCell(Cell metadata, Object cellValue) {
        if (!(cellValue instanceof Serializable)) {
            throw new DeepInstantiationException("provided cell value " + cellValue + " is not serializable");
        }

        this.cellName = metadata.getCellName();
        this.isClusterKey = ((CassandraCell) metadata).isClusterKey;
        this.isPartitionKey = ((CassandraCell) metadata).isPartitionKey;
        this.cellValidator = ((CassandraCell) metadata).cellValidator;
        this.cql3TypeClassName = cellValidator.getAbstractType().asCQL3Type().toString();
        this.cellValue = cellValue;
    }

    /**
     * Private constructor.
     */
    private CassandraCell(IDeepType e, Field field) {

        DeepField annotation = field.getAnnotation(DeepField.class);
        this.cellName = deepFieldName(field);
        this.cellValue = getBeanFieldValue(e, field);
        this.isClusterKey = annotation.isPartOfClusterKey();
        this.isPartitionKey = annotation.isPartOfPartitionKey();
        this.cellValidator = CellValidator.cellValidator(field);
        this.cql3TypeClassName = cellValidator.getAbstractType().asCQL3Type().toString();

    }


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

        CassandraCell cell = (CassandraCell) o;

        boolean isCellEqual = cellEquals(cell);
        boolean isKey = keyEquals(cell);
//        boolean isCellValidatorEqual = cellValidatorEquals(cell);
        boolean iscql3TypeClassNameEquals = cql3TypeClassNameEquals(cell);
        return isCellEqual && isKey && iscql3TypeClassNameEquals;
    }

    private boolean cellEquals(CassandraCell cell) {
        return cql3TypeClassName != null ? cql3TypeClassName.equals(cell.cql3TypeClassName) : cell.cql3TypeClassName == null;
    }

    private boolean cql3TypeClassNameEquals(CassandraCell cell) {
        return cellName.equals(cell.cellName) && cellValue != null ? cellValue.equals(cell.cellValue) : cell.cellValue == null;
    }

    private boolean keyEquals(CassandraCell cell) {
        return isClusterKey.equals(cell.isClusterKey) && isPartitionKey.equals(cell.isPartitionKey);
    }

    private boolean cellValidatorEquals(CassandraCell cell) {
        return cellValidator != null ? cellValidator.equals(cell.cellValidator) : cell.cellValidator == null;
    }


    public Class getValueType() {
        Class valueType = MAP_ABSTRACT_TYPE_CLASSNAME_TO_JAVA_TYPE.get(cellValidator.getValidatorClassName());
        if (valueType == null) {
            throw new DeepGenericException("Cannot find value type for marshaller " + cellValidator
                    .getValidatorClassName());
        }

        return valueType;
    }


    @Override
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
        result = 31 * result + (cellValidator != null ? cellValidator.hashCode() : 0);
        return result;
    }


    public Boolean isClusterKey() {
        return isClusterKey;
    }


    public Boolean isPartitionKey() {
        return isPartitionKey;
    }


    public Boolean isKey() {
        return isClusterKey || isPartitionKey;
    }


    public AbstractType marshaller() {
        if (cellValidator != null) {
            return cellValidator.getAbstractType();
        } else {
            return null;
        }
    }


    public String marshallerClassName() {
        if (cellValidator != null) {
            return cellValidator.getValidatorClassName();
        } else {
            return null;
        }
    }

    public String getCql3TypeClassName() {
        return cql3TypeClassName;
    }

    public void setCql3TypeClassName(String cql3TypeClassName) {
        this.cql3TypeClassName = cql3TypeClassName;
    }

    @Override
    public String toString() {

        final StringBuffer sb = new StringBuffer("CassandraCell{");
        sb.append("cellName='").append(cellName).append('\'');
        sb.append(", cellValue=").append(cellValue);
        sb.append("isPartitionKey=").append(isPartitionKey);
        sb.append(", isClusterKey=").append(isClusterKey);
        sb.append(", cellValidator=").append(cellValidator);
        sb.append(", cql3TypeClassName='").append(cql3TypeClassName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public void setCellValidator(CellValidator cellValidator) {
        this.cellValidator = cellValidator;
    }
}
