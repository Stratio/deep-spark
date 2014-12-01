/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.cassandra.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

import com.google.common.collect.ImmutableMap;

/**
 * Created by rcrespo on 26/11/14.
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
                    .put(UUID.class, UUIDType.instance)
                    .put(ByteBuffer.class, BytesType.instance)
                    .build();

    /**
     * Static map of associations between a cassandra marshaller fully qualified class name and the corresponding
     * Java class.
     */
    public static final Map<String, Class> MAP_ABSTRACT_TYPE_CLASSNAME_TO_JAVA_TYPE =
            ImmutableMap.<String, Class>builder()
                    .put(UTF8Type.class.getCanonicalName(), String.class)
                    .put(Int32Type.class.getCanonicalName(), Integer.class)
                    .put(BooleanType.class.getCanonicalName(), Boolean.class)
                    .put(TimestampType.class.getCanonicalName(), Date.class)
                    .put(DateType.class.getCanonicalName(), Date.class)
                    .put(DecimalType.class.getCanonicalName(), BigDecimal.class)
                    .put(LongType.class.getCanonicalName(), Long.class)
                    .put(DoubleType.class.getCanonicalName(), Double.class)
                    .put(FloatType.class.getCanonicalName(), Float.class)
                    .put(InetAddressType.class.getCanonicalName(), InetAddress.class)
                    .put(IntegerType.class.getCanonicalName(), BigInteger.class)
                    .put(UUIDType.class.getCanonicalName(), UUID.class)
                    .put(TimeUUIDType.class.getCanonicalName(), UUID.class)
                    .put(SetType.class.getCanonicalName(), Set.class)
                    .put(ListType.class.getCanonicalName(), List.class)
                    .put(MapType.class.getCanonicalName(), Map.class)
                    .put(BytesType.class.getCanonicalName(), ByteBuffer.class)
                    .build();

    /**
     * Static map of associations between cassandra marshaller Class objects and their instance.
     */
    public static final Map<Class<?>, AbstractType<?>> MAP_ABSTRACT_TYPE_CLASS_TO_ABSTRACT_TYPE =
            ImmutableMap.<Class<?>, AbstractType<?>>builder()
                    .put(UTF8Type.class, UTF8Type.instance)
                    .put(Int32Type.class, Int32Type.instance)
                    .put(BooleanType.class, BooleanType.instance)
                    .put(TimestampType.class, TimestampType.instance)
                    .put(DateType.class, DateType.instance)
                    .put(DecimalType.class, DecimalType.instance)
                    .put(LongType.class, LongType.instance)
                    .put(DoubleType.class, DoubleType.instance)
                    .put(FloatType.class, FloatType.instance)
                    .put(InetAddressType.class, InetAddressType.instance)
                    .put(IntegerType.class, IntegerType.instance)
                    .put(UUIDType.class, UUIDType.instance)
                    .put(TimeUUIDType.class, TimeUUIDType.instance)
                    .put(BytesType.class, BytesType.instance)
                    .build();

}
