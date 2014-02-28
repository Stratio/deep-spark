package com.stratio.deep.utils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;

import com.stratio.deep.annotations.DeepField;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.Pair;

/**
 * Created by luca on 26/02/14.
 */
public class AnnotationUtils {
    /**
     * Static map of associations between Class objects and the equivalent
     * Cassandra marshaller.
     */
    public static final Map<Class, AbstractType<?>> MAP_JAVA_TYPE_TO_ABSTRACT_TYPE =
		    ImmutableMap.<Class, AbstractType<?>>builder()
				    .put(String.class, UTF8Type.instance)
				    .put(Integer.class, Int32Type.instance)
				    .put(Boolean.class, BooleanType.instance)
				    .put(Date.class, TimestampType.instance)
				    .put(BigDecimal.class, DecimalType.instance)
				    .put(Long.class, LongType.instance)
				    .put(Double.class, DoubleType.instance)
				    .put(Float.class, FloatType.instance)
				    .put(InetAddress.class, InetAddressType.instance)
				    .put(Inet4Address.class, InetAddressType.instance)
				    .put(Inet6Address.class, InetAddressType.instance)
				    .put(BigInteger.class, IntegerType.instance)
				    .put(UUID.class, UUIDType.instance).build();

    public static final Map<Class<?>, AbstractType<?>> MAP_ABSTRACT_TYPE_CLASS_TO_ABSTRACT_TYPE =
		    ImmutableMap.<Class<?>, AbstractType<?>>builder()
				    .put(UTF8Type.class, UTF8Type.instance)
				    .put(Int32Type.class, Int32Type.instance)
				    .put(BooleanType.class, BooleanType.instance)
				    .put(TimestampType.class, TimestampType.instance)
				    .put(DecimalType.class, DecimalType.instance)
				    .put(LongType.class, LongType.instance)
				    .put(DoubleType.class, DoubleType.instance)
				    .put(FloatType.class, FloatType.instance)
				    .put(InetAddressType.class, InetAddressType.instance)
				    .put(IntegerType.class, IntegerType.instance)
				    .put(UUIDType.class, UUIDType.instance)
				    .put(TimeUUIDType.class, TimeUUIDType.instance)
				    .build();


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
     * Utility method that filters out all the fields _not_ annotated
     * with the {@link com.stratio.deep.annotations.DeepField} annotation.
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
     * Returns true is given field is part of the table key.
     *
     * @param field
     * @return
     */
    public static boolean isKey(DeepField field) {
	return field.isPartOfClusterKey() || field.isPartOfPartitionKey();
    }
}
