package com.stratio.deep.entity;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.utils.AnnotationUtils;
import org.apache.cassandra.db.marshal.*;

/**
 * Generic abstraction for cassandra's columns.
 * 
 * @author Luca Rosellini <luca@stratio.com>
 *
 * @param <T>
 */
public final class Cell<T extends Serializable> implements Serializable {

    private static final long serialVersionUID = 2298549804049316156L;

    /**
     * Name of the cell. Mapped to a Cassandra column name.
     */
    private String cellName;

    /**
     * Cell value.
     */
    private T cellValue;

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

    /**
     * Cassandra's validator class name for the current cell.<br/>
     * The provided type must extends {@link org.apache.cassandra.db.marshal.AbstractType}
     */
    private String validator = "org.apache.cassandra.db.marshal.UTF8Type";

    /**
     * Factory method, creates a new Cell.<br/>
     *
     * <p>
     * In validator is an UUID type this factory method will explicitly check if the provided
     * UUID serialized in the <i>value</i> ByteBuffer is an UUID or a Time UUID (UUID version 1).<br/>
     * In this case, the provided validator will be overridden and TimeUUIDType will be used instead.
     * </p>
     *
     * @param cellName the cell name
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @param validator the validator classname must be a valid qualified name of one of the marshallers
     *                  contained in the org.apache.cassandra.db.marshal package.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey true if this cell is part of the cassandra's clustering key.
     * @param <T>
     * @return
     */
    public static <T extends Serializable> Cell<T> create(String cellName, ByteBuffer cellValue, String validator,
		    Boolean isPartitionKey, Boolean isClusterKey) {
	return new Cell<T>(cellName, cellValue, validator, isPartitionKey, isClusterKey);
    }

    /**
     * Factory method, creates a new Cell.<br/>
     *
     * <p>
     * In validator is an UUID type this factory method will explicitly check if the provided
     * UUID serialized in the <i>value</i> ByteBuffer is an UUID or a Time UUID (UUID version 1).<br/>
     * In this case, the provided validator will be overridden and TimeUUIDType will be used instead.
     * </p>
     *
     * @param cellName the cell name
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @param validator the validator classname must be a valid qualified name of one of the marshallers
     *                  contained in the org.apache.cassandra.db.marshal package.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey true if this cell is part of the cassandra's clustering key.
     * @param <T>
     * @return
     */
    public static <T extends Serializable> Cell<T> create(String cellName, T cellValue, String validator,
		    Boolean isPartitionKey, Boolean isClusterKey) {
	return new Cell<T>(cellName, cellValue, validator, isPartitionKey, isClusterKey);
    }

    /**
     * Factory method, builds a new Cell (isPartitionKey = false and isClusterKey = false).
     * The validator will be automatically calculated using the value object type.
     *
     * @param cellName the cell name
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @param <T>
     * @return
     */
    public static <T extends Serializable> Cell<T> create(String cellName, T cellValue) {
	return create(cellName, cellValue, Boolean.FALSE, Boolean.FALSE);
    }

    /**
     * Factory method, creates a new Cell.<br/>
     *
     * @param cellName the cell name
     * @param cellValue the cell value, provided as a ByteBuffer.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey true if this cell is part of the cassandra's clustering key.
     * @param <T>
     * @return
     */
    public static <T extends Serializable> Cell<T> create(String cellName, T cellValue, Boolean isPartitionKey,
	    Boolean isClusterKey) {
	return new Cell<T>(cellName, cellValue, isPartitionKey, isClusterKey);
    }

    /**
     * Factory method, creates a new metadata Cell, i.e. a Cell without value.
     *
     * @param cellName the cell name
     * @param cellType the cell value type.
     * @param isPartitionKey true if this cell is part of the cassandra's partition key.
     * @param isClusterKey true if this cell is part of the cassandra's clustering key.
     * @return
     */
    public static Cell<?> createMetadataCell(String cellName, Class<?> cellType, Boolean isPartitionKey,
		    Boolean isClusterKey) {
	return new Cell(cellName, cellType, isPartitionKey, isClusterKey);
    }

    /**
     * Calculates the Cassandra validator type given the value class type.<br/>
     * There's a shortcoming in the case of an UUID. At this level we are not able
     * to distinguish between an UUID or a TimeUUID because twe don't have the UUID value.
     *
     * @param obj the value class type.
     * @param <T>
     * @return
     */
    private static <T extends Serializable> AbstractType<?> getValueType(Class<T> obj) {
	if (obj == null) {
	    return UTF8Type.instance;
	}

	return AnnotationUtils.MAP_JAVA_TYPE_TO_ABSTRACT_TYPE.get(obj);
    }

    /**
     * Calculates the Cassandra marshaller given the cell value.
     *
     * @param obj the cell value.
     * @param <T>
     * @return
     */
    private static <T extends Serializable> AbstractType<?> getValueType(T obj) {
	if (obj == null) {
	    return UTF8Type.instance;
	}

	AbstractType<?> res = AnnotationUtils.MAP_JAVA_TYPE_TO_ABSTRACT_TYPE.get(obj.getClass());

	if (res == null){
	    throw new DeepGenericException("parameter class "+obj.getClass().getCanonicalName()+"does not have a Cassandra marshaller");
	}

	if (obj instanceof UUID) {
	    UUID uuid = (UUID) obj;

	    if (uuid.version() == 1) {
		return TimeUUIDType.instance;
	    } else {
		return UUIDType.instance;
	    }
	}

	return res;
    }

    /**
     * Returns an instance of a cassandra validator given its canonical name.
     *
     * @param valueAbstractType
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> AbstractType<T> marshaller(String valueAbstractType) {
	try {
	    Class<AbstractType<T>> clazz = (Class<AbstractType<T>>) Class.forName(valueAbstractType);
	    Field f = clazz.getField("instance");
	    return (AbstractType<T>) f.get(null);

	} catch (ClassNotFoundException e) {
	    throw new DeepGenericException(e);
	} catch (NoSuchFieldException e) {
	    throw new DeepNoSuchFieldException(e);
	} catch (IllegalAccessException e) {
	    throw new DeepIllegalAccessException(e);
	}
    }

    /**
     * Private constructor.
     */
    private Cell(String cellName, ByteBuffer cellValue, String validator, Boolean isPartitionKey,
	    Boolean isClusterKey) {
	this.cellName = cellName;

	this.isPartitionKey = isPartitionKey;
	this.isClusterKey = isClusterKey;
	this.validator = validator;

	if (this.validator.equals(UUIDType.class.getCanonicalName())){
	    // check if it's a TimeUUID
	    UUID id = UUIDType.instance.compose(cellValue);

	    if (id.version() == 1 /* time UUID */ ) {
		this.validator = TimeUUIDType.class.getCanonicalName();
	    }
	}

	if (cellValue != null) {
	    this.cellValue = marshaller().compose(cellValue);
	}
    }

    /**
     * Private constructor.
     */
    private Cell(String cellName, T cellValue, String validator, Boolean isPartitionKey,
		    Boolean isClusterKey) {
	this.cellName = cellName;
	this.isPartitionKey = isPartitionKey;
	this.isClusterKey = isClusterKey;
	this.validator = validator;
	this.cellValue = cellValue;
    }

    /**
     * Private constructor.
     */
    private Cell(String cellName, T cellValue, Boolean isPartitionKey, Boolean isClusterKey) {
	this.cellName = cellName;
	this.cellValue = cellValue;
	this.isClusterKey = isClusterKey;
	this.isPartitionKey = isPartitionKey;
	this.validator = getValueType(cellValue).getClass().getCanonicalName();
    }

    /**
     * Private constructor.
     */
    private Cell(String cellName, Class<T> cellType, Boolean isPartitionKey, Boolean isClusterKey) {
	this.cellName = cellName;
	this.isClusterKey = isClusterKey;
	this.isPartitionKey = isPartitionKey;
	this.validator = getValueType(cellType).getClass().getCanonicalName();
    }

    /**
     * Two Cell are considered equal if and only if the following properties are equal:
     * <ol>
     *     <li>cellName</li>
     *     <li>cellValue</li>
     *     <li>isClusterKey</li>
     *     <li>isPartitionKey</li>
     *     <li>validator</li>
     * </ol>
     *
     * @param o
     * @return
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
			validator.equals(cell.validator);
    }

    public Class<T> getValueType(){
	for (Map.Entry<Class, AbstractType<?>> entry : AnnotationUtils.MAP_JAVA_TYPE_TO_ABSTRACT_TYPE.entrySet()) {
	    if (entry.getValue().getClass().getCanonicalName().equals(validator)){
		return entry.getKey();
	    }
	}

	return null;
    }

    public String getCellName() {
	return cellName;
    }

    public T getCellValue() {
	return cellValue;
    }

    /**
     * Returns the cell value as a ByteBuffer, performs the conversion using the
     * configured validator.
     *
     * If cell value is null we propagate an empty array, see CASSANDRA-5885 and CASSANDRA-6180.
     *
     * @return
     */
    public ByteBuffer getDecomposedCellValue() {

	if (this.cellValue != null) {
	    return marshaller().decompose(this.cellValue);
	} else {
	    /* if null we propagate an empty array, see CASSANDRA-5885 and CASSANDRA-6180 */
	    return ByteBuffer.wrap(new byte[0]);
	}
    }

    /**
     * calcs the cell hash code.
     *
     * @return
     */
    @Override
    public int hashCode() {
	int result = cellName.hashCode();
	result = 31 * result + (cellValue != null ? cellValue.hashCode() : 0);
	result = 31 * result + isPartitionKey.hashCode();
	result = 31 * result + isClusterKey.hashCode();
	result = 31 * result + validator.hashCode();
	return result;
    }

    public Boolean isClusterKey() {
	return isClusterKey;
    }

    public Boolean isPartitionKey() {
	return isPartitionKey;
    }

    public AbstractType<T> marshaller() {
	return marshaller(this.validator);
    }

    public String marshallerClassName(){
	return this.validator;
    }

    @Override
    public String toString() {
	return "Cell{" + "cellName='" + cellName + '\'' + ", cellValue=" + cellValue + ", isPartitionKey="
		+ isPartitionKey + ", isClusterKey=" + isClusterKey + ", validator='" + validator + '\'' + '}';
    }

}
