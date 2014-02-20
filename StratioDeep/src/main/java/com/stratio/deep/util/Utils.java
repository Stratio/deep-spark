package com.stratio.deep.util;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;

import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import org.apache.cassandra.utils.Pair;
import scala.Tuple2;

/**
 * Utility class providing useful methods to manipulate the conversion
 * between ByteBuffers maps coming from the underlying Cassandra API to
 * instances of a concrete javabean.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class Utils {

    /**
     * Utility method that converts an IDeepType to tuple of two Cells.<br/>
     * The first Cells element contains the list of Cell elements that represent the key (partition + cluster key). <br/>
     * The second Cells element contains all the other columns.
     *
     * @param cells
     * @return
     */
    public static Tuple2<Cells, Cells> cellList2tuple(Cells cells) {
	Cells keys = new Cells();
	Cells values = new Cells();

	for (Cell<?> c : cells) {

	    if (c.isPartitionKey() || c.isClusterKey()) {

		keys.add(c);
	    } else {
		values.add(c);
	    }
	}

	return new Tuple2<>(keys, values);
    }

    /**
     * Returns the field name as known by the datastore.
     *
     * @param field
     * @return
     */
    public static String deepFieldName(Field field) {

	DeepField annotation = field.getAnnotation(DeepField.class);
	if (StringUtils.isNotEmpty(annotation.fieldName())) {
	    return annotation.fieldName();
	} else {
	    return field.getName();
	}
    }

    /**
     * Convers an instance of type <T> to a tuple of ( Map<String, ByteBuffer>, List<ByteBuffer> ).
     * The first map contains the key column names and the corresponding values.
     * The ByteBuffer list contains the value of the columns that will be bounded to CQL query parameters.
     *
     * @param e
     * @param <T>
     * @return
     */
    public static <T extends IDeepType> Tuple2<Cells, Cells> deepType2tuple(T e) {

	Pair<Field[], Field[]> fields = filterKeyFields(e.getClass().getDeclaredFields());

	Field[] keyFields = fields.left;
	Field[] otherFields = fields.right;

	Cells keys = new Cells();
	Cells values = new Cells();

	for (Field keyField : keyFields) {
	    Serializable value = getBeanFieldValue(e, keyField);
	    DeepField deepField = keyField.getAnnotation(DeepField.class);

	    if (value != null) {
		keys.add(Cell.create(deepFieldName(keyField), value, deepField.isPartOfPartitionKey(),
				deepField.isPartOfClusterKey()));
	    }
	}

	for (Field valueField : otherFields) {
	    Serializable value = getBeanFieldValue(e, valueField);
	    DeepField deepField = valueField.getAnnotation(DeepField.class);
	    values.add(Cell.create(deepFieldName(valueField), value, deepField.isPartOfPartitionKey(),
			    deepField.isPartOfClusterKey()));
	}

	return new Tuple2<>(keys, values);
    }

    /**
     * Utility method that filters out all the fields _not_ annotated
     * with the {@link DeepField} annotation.
     *
     * @param fields
     * @return
     */
    public static Field[] filterDeepFields(Field[] fields) {
	List<Field> filtered = new ArrayList<>();
	for (Field f : fields) {
	    if (f.isAnnotationPresent(DeepField.class)) {
		filtered.add(f);
	    }
	}
	return filtered.toArray(new Field[filtered.size()]);
    }

    /**
     * Return a pair of Field[] whose left element is
     * the array of keys fields.
     * The right element contains the array of all other non-key fields.
     *
     * @param fields
     * @return
     */
    public static Pair<Field[], Field[]> filterKeyFields(Field[] fields) {
	Field[] filtered = filterDeepFields(fields);
	List<Field> keys = new ArrayList<>();
	List<Field> others = new ArrayList<>();

	for (Field field : filtered) {
	    if (isKey(field.getAnnotation(DeepField.class))) {
		keys.add(field);
	    } else {
		others.add(field);
	    }
	}

	return Pair.create(keys.toArray(new Field[keys.size()]), others.toArray(new Field[others.size()]));
    }

    /**
     * Returns the value of the fields <i>f</i> in the instance <i>e</i> of type T.
     *
     * @param e
     * @param f
     * @param <T>
     * @return
     */
    public static <T extends IDeepType> Serializable getBeanFieldValue(T e, Field f) {
	try {
	    return (Serializable) PropertyUtils.getProperty(e, f.getName());

	} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e1) {
	    throw new DeepIOException(e1);
	}

    }

    /**
     * Returns true is given field is part of the table key.
     *
     * @param field
     * @return
     */
    public static boolean isKey(DeepField field) {
	return field.isPartOfClusterKey() || field.isPartOfPartitionKey();
    }

    /**
     * Creates a new instance of the given class.
     *
     * @param clazz the class object for which a new instance should be created.
     * @return the new instance of class clazz.
     */
    public static <T extends IDeepType> T newTypeInstance(Class<T> clazz) {
	try {
	    return clazz.newInstance();
	} catch (InstantiationException | IllegalAccessException e) {
	    throw new DeepGenericException(e);
	}
    }

    /**
     * Quoting for working with uppercase
     */
    public static String quote(String identifier) {
	return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }

    public static String additionalFilterGenerator(Map<String, Serializable> additionalFilters){
	if (additionalFilters == null || additionalFilters.size() == 0){
	    return null;
	}

	StringBuilder sb = new StringBuilder();

	for (Map.Entry<String, Serializable> entry : additionalFilters.entrySet()) {
	    String value = entry.getValue().toString();
	    if ((entry.getValue() instanceof  String)){
		value = value.trim();
		if (!value.startsWith("'")){
		    value = "'" + value;
		}
		if (!value.endsWith("'")){
		    value = value + "'";
		}
	    }

	    sb.append(" AND ").append(quote(entry.getKey())).append(" = ").append(value);
	}

	return sb.toString();
    }

    /**
     * Generates the update query for the provided IDeepType.
     * The UPDATE query takes into account all the columns of the entity, even those containing the null value.
     * We do not generate the key part of the update query. The provided query will be concatenated with the key part
     * by CqlRecordWriter.
     *
     * @param outputKeyspace
     * @param outputColumnFamily
     * @param <T>
     * @return
     */
    public static <T extends IDeepType> String updateQueryGenerator(Cells keys, Cells values, String outputKeyspace,
		    String outputColumnFamily) {

	StringBuilder sb = new StringBuilder("UPDATE ").append(outputKeyspace).append(".").append(outputColumnFamily)
			.append(" SET ");

	int k = 0;

	StringBuilder keyClause = new StringBuilder(" WHERE ");
	for (Cell<?> cell : keys.getCells()) {
	    if (cell.isPartitionKey() || cell.isClusterKey()) {
		if (k > 0) {
		    keyClause.append(" AND ");
		}

		keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

		++k;
	    }

	}

	k = 0;
	for (Cell<?> cell : values.getCells()) {
	    if (k > 0) {
		sb.append(", ");
	    }

	    sb.append(String.format("%s = ?", quote(cell.getCellName())));
	    ++k;
	}

	sb.append(keyClause);

	return sb.toString();
    }

    private Utils() {

    }

}
