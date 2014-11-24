package com.stratio.deep.cassandra.entity;

import static com.stratio.deep.commons.utils.AnnotationUtils.MAP_ABSTRACT_TYPE_CLASSNAME_TO_JAVA_TYPE;
import static com.stratio.deep.commons.utils.AnnotationUtils.deepFieldName;
import static com.stratio.deep.commons.utils.AnnotationUtils.getBeanFieldValue;
import org.apache.cassandra.db.marshal.MapType;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.stratio.deep.cassandra.util.CassandraUtils;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepInstantiationException;

/**
 * Created by rcrespo on 23/06/14.
 */
@Deprecated
public class CassandraCell extends Cell {



    private transient CellValidator cellValidator;

    private transient DataType dataType;



    /**
     * Factory method, creates a new CassandraCell from a basic Cell. The keys in the cell will be partitionsKey<br/>
     *
     * @param basicCell  the cell object carrying the metadata and the value
     * @return an instance of a Cell object for the provided parameters.
     */
    public static Cell create(Cell basicCell) {
        //TODO if basicCell instanceof CassandraCell => return basicCell;
        return new CassandraCell(basicCell);
    }


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
        this.isKey = isPartitionKey;
        this.isClusterKey = isClusterKey;
        this.cellValidator = getValueType(cellValue);

    }

    /**
     * Private constructor.
     */
    private CassandraCell(String cellName, DataType cellType, Boolean isPartitionKey, Boolean isClusterKey) {
        this.cellName = cellName;
        this.isClusterKey = isClusterKey;
        this.dataType = cellType;
        this.isKey = isPartitionKey;

    }

    public DataType getDataType() {
        return dataType;
    }

    /**
     * Private constructor.
     */
    private CassandraCell(Cell metadata, ByteBuffer cellValue) {
        this.cellName = metadata.getCellName();
        this.isClusterKey = ((CassandraCell) metadata).isClusterKey;
        this.isKey = ((CassandraCell)metadata).isPartitionKey();
        this.cellValidator = ((CassandraCell) metadata).cellValidator;
        if (cellValue != null) {
            if(((CassandraCell)metadata).getDataType()!=null){
                this.cellValue = ((CassandraCell)metadata).getDataType().deserialize(cellValue, ProtocolVersion.V2);
            }else{
                this.cellValue = ((CassandraCell) metadata).marshaller().compose(cellValue);
            }
        }
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
        this.isKey = ((CassandraCell)metadata).isPartitionKey();
        this.cellValidator = ((CassandraCell) metadata).cellValidator;
        this.cellValue = cellValue;
    }

    /**
     * Private constructor.Cassandra cell from a basic cell.
     */
    private CassandraCell(Cell cell) {
            this.cellName = cell.getCellName();
            this.cellValue = cell.getCellValue();
            this.isKey = cell.isKey();
            this.cellValidator = getValueType(cellValue);
    }


    /**
     * Private constructor.
     */
    private CassandraCell(IDeepType e, Field field) {
        DeepField annotation = field.getAnnotation(DeepField.class);
        this.cellName = deepFieldName(field);
        this.cellValue = getBeanFieldValue(e, field);
        this.isClusterKey = annotation.isPartOfClusterKey();
        this.isKey = annotation.isPartOfPartitionKey();
        this.cellValidator = CellValidator.cellValidator(field);

    }

    public CellValidator getCellValidator() {
        return cellValidator;
    }





    public Class getValueType() {
        Class valueType = MAP_ABSTRACT_TYPE_CLASSNAME_TO_JAVA_TYPE.get(cellValidator.getValidatorClassName());
        if (valueType == null) {
            throw new DeepGenericException("Cannot find value type for marshaller " + cellValidator
                    .getValidatorClassName());
        }

        return valueType;
    }

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
        result = 31 * result + isKey.hashCode();
        result = 31 * result + isClusterKey.hashCode();
        result = 31 * result + (cellValidator != null ? cellValidator.hashCode() : 0);
        return result;
    }

    public Boolean isClusterKey() {
        return isClusterKey;
    }

    public Boolean isPartitionKey() {
        return isKey;
    }


    public Boolean isKey() {
        return isClusterKey || isKey;
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



    @Override
    public String toString() {

        final StringBuffer sb = new StringBuffer("CassandraCell{");
        sb.append("cellName='").append(cellName).append('\'');
        sb.append(", cellValue=").append(cellValue);
        sb.append(", isPartitionKey=").append(isKey);
        sb.append(", isClusterKey=").append(isClusterKey);
        sb.append(", cellValidator=").append(cellValidator);
        sb.append('}');
        return sb.toString();
    }

}
