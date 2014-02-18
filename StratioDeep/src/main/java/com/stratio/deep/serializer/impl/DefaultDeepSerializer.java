package com.stratio.deep.serializer.impl;

import static com.stratio.deep.util.CassandraRDDUtils.*;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.AbstractType;

import com.stratio.deep.entity.DeepByteBuffer;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.entity.impl.DeepByteBufferImpl;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.serializer.IDeepSerializer;

/**
 * Default {@link IDeepSerializer} implementation.
 * 
 * @author Luca Rosellini <luca@strat.io>
 *
 * @param <T>
 */
public class DefaultDeepSerializer<T extends IDeepType> implements IDeepSerializer<T> {

    /*
     * (non-Javadoc)
     * @see com.stratio.deep.serializer.IDeepSerializer#deserialize(byte[])
     */
    @Override
    public Object deserialize(DeepByteBuffer<?> byteBuffer) {
	return byteBuffer.getObject();
    }

    /*
     * (non-Javadoc)
     * @see com.stratio.deep.serializer.IDeepSerializer#serialize(java.util.Map.Entry, java.lang.Class)
     */
    @Override
    public DeepByteBuffer<?> serialize(Entry<String, ByteBuffer> entry, Class<T> clazz) {
	if (entry == null) {
	    throw new DeepIllegalAccessException("entry cannot be null");
	}

	if (entry.getValue() == null) {
	    return null;
	}

	Field f = deepField(entry.getKey(), clazz);

	if (f == null) {
	    return null;
	}

	AbstractType<?> typeInstance = cassandraMarshaller(f);
	Serializable obj = (Serializable) typeInstance.compose(entry.getValue());

	return DeepByteBufferImpl.create(f.getName(), f.getDeclaringClass(), obj);
    }
}
