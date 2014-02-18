package com.stratio.deep.serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.DeepByteBuffer;
import com.stratio.deep.entity.IDeepType;

/**
 * Contract that all serializer must obey in order to be used
 * for serializing/deserialized fields of IDeepType objects.
 * 
 * Beware of the meaning of the generic type T: an implementation of this 
 * interface does NOT serialize a whole instance of an IDeepType, it is merely
 * used to serialize each field of an {@link IDeepType} marked with the {@link DeepField}
 * annotation.
 * The Class<T> parameter is needed in order to detect which cassandra marshaller
 * needs to be used. The name of the field is between the Map.Entry and the Class<T> object
 * is done using the field name.
 * 
 * @author Luca Rosellini <luca@strat.io>
 *
 * @param <T>
 */
public interface IDeepSerializer<T> {

    Object deserialize(DeepByteBuffer<?> byteBuffer);

    /**
     * Gets a cassandra field (coming from the underlying Cassandra API)
     * as {@link ByteBuffer} and converts it to a serializable {@link DeepByteBuffer}.
     * 
     * In order to perform the seialisation the following steps occur:
     * <ol>
     * <li>Uses Reflection to obtain from <i>clazz</i> the {@link Field} whose name <br/>
     * or {@link DeepField#fieldName()} matches entry.getKey(). <br/>
     * If we cannot find a match, null is immediately returned.</li>
     * <li>Gets a Cassandra marshaller matching {@link DeepField#annotationType()}.</li>
     * <li>Serializes the field value to a byte[]</li>
     * <li>Instantiates and return a new {@link DeepByteBuffer}.</li>
     * </ol>
     * 
     * @param entry
     * @param clazz
     * @return
     */
    DeepByteBuffer<?> serialize(Map.Entry<String, ByteBuffer> entry, Class<T> clazz);
}
