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

package com.stratio.deep.cassandra.entity;

import static java.lang.Class.forName;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.commons.collections.CollectionUtils;

import com.datastax.driver.core.DataType;
import com.google.common.collect.ImmutableMap;
import com.stratio.deep.cassandra.util.CassandraUtils;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.exception.DeepInstantiationException;
import com.stratio.deep.commons.utils.AnnotationUtils;

/**
 * Defines a serializable CellValidator. <br/>
 * <p/>
 * This object wraps the complexity of obtaining the Cassandra's AbstractType
 * associated to a {@link com.stratio.deep.commons.entity.Cell}.
 * <p/>
 * In the case of collection types, a simple cassandra marshaller qualified name
 * is not enough to fully generate an AbstractType, we also need the type(s) the
 * collection holds.
 */
public class CellValidator {
    private static final String DEFAULT_VALIDATOR_CLASSNAME = "org.apache.cassandra.db.marshal.UTF8Type";

    public static final Map<Class, CQL3Type.Native> MAP_JAVA_TYPE_TO_CQL_TYPE =
            ImmutableMap.<Class, CQL3Type.Native>builder()
                    .put(String.class, CQL3Type.Native.TEXT)
                    .put(Integer.class, CQL3Type.Native.INT)
                    .put(Boolean.class, CQL3Type.Native.BOOLEAN)
                    .put(Date.class, CQL3Type.Native.TIMESTAMP)
                    .put(BigDecimal.class, CQL3Type.Native.DECIMAL)
                    .put(Long.class, CQL3Type.Native.BIGINT)
                    .put(Double.class, CQL3Type.Native.DOUBLE)
                    .put(Float.class, CQL3Type.Native.FLOAT)
                    .put(InetAddress.class, CQL3Type.Native.INET)
                    .put(Inet4Address.class, CQL3Type.Native.INET)
                    .put(Inet6Address.class, CQL3Type.Native.INET)
                    .put(BigInteger.class, CQL3Type.Native.VARINT)
                    .put(UUID.class, CQL3Type.Native.UUID)
                    .build();

    private static final Map<String, DataType.Name> MAP_JAVA_TYPE_TO_DATA_TYPE_NAME =
            ImmutableMap.<String, DataType.Name>builder()
                    .put(UTF8Type.class.getCanonicalName(), DataType.Name.TEXT)
                    .put(Int32Type.class.getCanonicalName(), DataType.Name.INT)
                    .put(BooleanType.class.getCanonicalName(), DataType.Name.BOOLEAN)
                    .put(TimestampType.class.getCanonicalName(), DataType.Name.TIMESTAMP)
                    .put(DateType.class.getCanonicalName(), DataType.Name.TIMESTAMP)
                    .put(DecimalType.class.getCanonicalName(), DataType.Name.DECIMAL)
                    .put(LongType.class.getCanonicalName(), DataType.Name.BIGINT)
                    .put(DoubleType.class.getCanonicalName(), DataType.Name.DOUBLE)
                    .put(FloatType.class.getCanonicalName(), DataType.Name.FLOAT)
                    .put(InetAddressType.class.getCanonicalName(), DataType.Name.INET)
                    .put(IntegerType.class.getCanonicalName(), DataType.Name.VARINT)
                    .put(UUIDType.class.getCanonicalName(), DataType.Name.UUID)
                    .put(TimeUUIDType.class.getCanonicalName(), DataType.Name.TIMEUUID)
                    .put(MapType.class.getCanonicalName(), DataType.Name.MAP)
                    .put(SetType.class.getCanonicalName(), DataType.Name.SET)
                    .put(ListType.class.getCanonicalName(), DataType.Name.LIST)
                    .put(BytesType.class.getCanonicalName(), DataType.Name.BLOB)
                    .build();

    /**
     * Possible types of CellValidator.
     */
    public static enum Kind {
        MAP, SET, LIST, NOT_A_COLLECTION;

        /**
         * Returns the Kind associated to the provided Cassandra validator class.
         * <p/>
         * <p>
         * To be more specific, returns:
         * <ul>
         * <li>NOT_A_COLLECTION: when the provided type is not a set a list or a map</li>
         * <li>MAP: when the provided validator is MapType.class</li>
         * <li>LIST: when the provided validator is ListType.class</li>
         * <li>SET: when the provided validator is SetType.class</li>
         * </ul>
         * </p>
         *
         * @param type the marshaller Class object.
         * @return the Kind associated to the provided marhaller Class object.
         */
        public static Kind validatorClassToKind(Class<? extends AbstractType> type) {
            if (type == null) {
                return NOT_A_COLLECTION;
            }

            if (type.equals(SetType.class)) {
                return SET;
            } else if (type.equals(ListType.class)) {
                return LIST;
            } else if (type.equals(MapType.class)) {
                return MAP;
            } else {
                return NOT_A_COLLECTION;
            }
        }

        /**
         * Returns the Kind associated to the provided object instance.
         * <p/>
         * <p>To be more specific, returns:
         * <ul>
         * <li>{@link CellValidator.Kind#SET}: when the provided instance object is an
         * instance of {@link java.util.Set}</li>
         * <li>{@link CellValidator.Kind#LIST}: when the provided instance object is an
         * instance of {@link java.util.List}</li>
         * <li>{@link CellValidator.Kind#MAP}: when the provided instance object is an
         * instance of {@link java.util.Map}</li>
         * <li>{@link CellValidator.Kind#NOT_A_COLLECTION}: otherwise.</li>
         * </ul>
         * </p>
         *
         * @param object an object instance.
         * @param <T>    the generic type of the provided object instance.
         * @return the Kind associated to the provided object.
         */
        public static <T> Kind objectToKind(T object) {
            if (object == null) {
                return NOT_A_COLLECTION;
            }

            Kind res;

            if (Set.class.isAssignableFrom(object.getClass())) {
                res = SET;
            } else if (List.class.isAssignableFrom(object.getClass())) {
                res = LIST;
            } else if (Map.class.isAssignableFrom(object.getClass())) {
                res = MAP;
            } else {
                res = NOT_A_COLLECTION;
            }

            return res;
        }
    }

    /**
     * Cassandra's validatorClassName class name for the current cell.<br/>
     * The provided type must extends {@link org.apache.cassandra.db.marshal.AbstractType}
     */
    private String validatorClassName = DEFAULT_VALIDATOR_CLASSNAME;

    /**
     * Only applies to collection types (validatorKind != Kind.NOT_A_COLLECTION), contains the list of
     * types the collection holds. At most, this collection will contain two elements, in the case
     * of validatorKind == Kind.MAP.
     */
    private Collection<String> validatorTypes;

    /**
     * Holds the type of the current validator.
     */
    private Kind validatorKind = Kind.NOT_A_COLLECTION;

    private transient AbstractType<?> abstractType;

    private DataType.Name cqlTypeName;

    /**
     * Factory method that builds a CellValidator from an IDeepType field.
     *
     * @param field the IDeepType field.
     * @return a new CellValidator associated to the provided object.
     */
    public static CellValidator cellValidator(Field field) {
        return new CellValidator(field);
    }

    /**
     * Factory method that builds a CellValidator from a DataType object.
     *
     * @param type the data type coming from the driver.
     * @return a new CellValidator associated to the provided object.
     */
    public static CellValidator cellValidator(DataType type) {
        return new CellValidator(type);
    }

    /**
     * Generates a CellValidator for a generic instance of an object.
     * We need the actual instance in order to differentiate between an UUID and a TimeUUID.
     *
     * @param obj an instance to use to build the new CellValidator.
     * @param <T> the generic type of the provided object instance.
     * @return a new CellValidator associated to the provided object.
     */
    public static <T> CellValidator cellValidator(T obj) {
        if (obj == null) {
            return null;
        }

        Kind kind = Kind.objectToKind(obj);
        AbstractType<?> tAbstractType = CassandraUtils.marshallerInstance(obj);
        String validatorClassName = tAbstractType.getClass().getCanonicalName();
        Collection<String> validatorTypes = null;
        DataType.Name cqlTypeName = MAP_JAVA_TYPE_TO_DATA_TYPE_NAME.get(validatorClassName);// tAbstractType.get

        return new CellValidator(validatorClassName, kind, validatorTypes, cqlTypeName);
    }

    /**
     * private constructor.
     */
    private CellValidator(String validatorClassName, Kind validatorKind, Collection<String> validatorTypes,
            DataType.Name cqlTypeName) {
        this.validatorClassName = validatorClassName != null ? validatorClassName : DEFAULT_VALIDATOR_CLASSNAME;
        this.validatorKind = validatorKind;
        this.validatorTypes = validatorTypes;
        this.cqlTypeName = cqlTypeName;
    }

    /**
     * private constructor.
     */
    private static String getCollectionInnerType(Class<?> type) {
        CQL3Type.Native nativeType = MAP_JAVA_TYPE_TO_CQL_TYPE.get(type);
        return nativeType.name().toLowerCase();
    }

    /**
     * private constructor.
     */
    private CellValidator(Field field) {
        Class<?>[] types = AnnotationUtils.getGenericTypes(field);
        DeepField annotation = field.getAnnotation(DeepField.class);

        Class<? extends AbstractType> clazz = annotation.validationClass();

        this.validatorClassName = annotation.validationClass().getCanonicalName();
        this.validatorKind = Kind.validatorClassToKind(clazz);
        cqlTypeName = MAP_JAVA_TYPE_TO_DATA_TYPE_NAME.get(this.validatorClassName);

        switch (this.validatorKind) {
        case SET:
        case LIST:
            this.validatorTypes = asList(getCollectionInnerType(types[0]));
            break;
        case MAP:
            this.validatorTypes = asList(getCollectionInnerType(types[0]), getCollectionInnerType(types[1]));
            break;
        default:
        }
    }

    /**
     * private constructor.
     *
     * @param type a {@link com.datastax.driver.core.DataType} coming from the underlying Java driver.
     */
    private CellValidator(DataType type) {
        if (type == null) {
            throw new DeepInstantiationException("input DataType cannot be null");
        }

        cqlTypeName = type.getName();

        if (!type.isCollection()) {
            validatorClassName = AnnotationUtils.MAP_JAVA_TYPE_TO_ABSTRACT_TYPE.get(type.asJavaClass()).getClass()
                    .getName();
        } else {
            validatorTypes = new ArrayList<>();

            for (DataType dataType : type.getTypeArguments()) {
                validatorTypes.add(dataType.toString());
            }

            switch (type.getName()) {
            case SET:
                validatorKind = Kind.SET;
                validatorClassName = SetType.class.getName();
                break;
            case LIST:
                validatorKind = Kind.LIST;
                validatorClassName = ListType.class.getName();
                break;
            case MAP:
                validatorKind = Kind.MAP;
                validatorClassName = MapType.class.getName();
                break;
            default:
                throw new DeepGenericException("Cannot determine collection type for " + type.getName());
            }

            validatorTypes = unmodifiableCollection(validatorTypes);
        }
    }

    /**
     * Generates the cassandra marshaller ({@link org.apache.cassandra.db.marshal.AbstractType}) for this CellValidator.
     *
     * @return an instance of the cassandra marshaller for this CellValidator.
     */
    public AbstractType<?> getAbstractType() {
        if (abstractType != null) {
            return abstractType;
        }

        try {

            if (validatorKind == Kind.NOT_A_COLLECTION) {
                abstractType = AnnotationUtils.MAP_ABSTRACT_TYPE_CLASS_TO_ABSTRACT_TYPE.get(forName
                        (validatorClassName));

            } else {
                throw new DeepGenericException("Cannot determine collection kind for " + validatorKind);

                }


        } catch (ClassNotFoundException e) {
            throw new DeepGenericException(e);
        }

        return abstractType;
    }

    /**
     * Getter for validatorClassName.
     *
     * @return the cassandra marshaller class name.
     */
    public String getValidatorClassName() {
        return validatorClassName;
    }

    /**
     * Getter for validatorTypes.
     *
     * @return a collection of validator type names.
     */
    public Collection<String> getValidatorTypes() {
        return validatorTypes;
    }

    /**
     * Getter for validatorKind.
     *
     * @return the Kind associated to this CellValidator.
     */
    public Kind validatorKind() {
        return validatorKind;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CellValidator that = (CellValidator) o;

        if (validatorKind != that.validatorKind) {
            return false;
        }
        if (!validatorClassName.equals(that.validatorClassName)) {
            return false;
        }
        if (validatorKind != Kind.NOT_A_COLLECTION &&
                !CollectionUtils.isEqualCollection(validatorTypes, that.validatorTypes)) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = validatorClassName.hashCode();
        result = 31 * result + (validatorTypes != null ? validatorTypes.hashCode() : 0);
        result = 31 * result + validatorKind.hashCode();
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "CellValidator{" +
                "validatorClassName='" + validatorClassName + '\'' +
                ", validatorTypes=" + (validatorTypes != null ? validatorTypes : "") +
                ", validatorKind=" + validatorKind +
                ", abstractType=" + abstractType +
                '}';
    }

    /**
     * @return the original CQL3 type name (if known, null otherwise)
     */
    public DataType.Name getCqlTypeName() {
        return cqlTypeName;
    }
}
