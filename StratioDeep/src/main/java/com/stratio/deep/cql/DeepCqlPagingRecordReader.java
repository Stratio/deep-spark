package com.stratio.deep.cql;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.cassandra.utils.Pair;

/**
 * Extends Cassandra's CqlPagingRecordReader in order to make it
 * more iterator friendly: re-implements the ugly (nextKeyValue() + getCurrentKey() + getCurrentValue())
 * behavior in the more standard hasNext() + next(), with hasNext() not increasing the iterator
 * current position.
 *
 * @author Luca Rosellini <luca@strat.io>
 *
 */
public class DeepCqlPagingRecordReader extends CqlPagingRecordReader{
    private static final String UNDERLYING_ROW_ITERATOR_NAME = "rowIterator";

    /**
     * Very ugly trick used to access rowIterator private method.
     *
     * To Cassandra's developers: please, be a little bit more friendly with
     * all those poor folks like us trying to build upon you infrastructure.
     */
    @SuppressWarnings("unchecked")
    private Iterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> getPrivateRowIterator() {
	Field f = null;
	try {
	    f = CqlPagingRecordReader.class.getDeclaredField(UNDERLYING_ROW_ITERATOR_NAME);
	} catch (NoSuchFieldException nsfe) {
	    throw new DeepNoSuchFieldException("Could not find field "+UNDERLYING_ROW_ITERATOR_NAME,nsfe);
	}

	boolean originalAccessor = f.isAccessible();

	f.setAccessible(true);
	try {
	    return (Iterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>>) f.get(this);
	} catch (IllegalAccessException iae) {
	    throw new DeepIllegalAccessException("Underlying iterator " + UNDERLYING_ROW_ITERATOR_NAME + " cannot be accessed even though its accessibility has been changed.",iae);
	} finally {
	    f.setAccessible(originalAccessor);
	}
    }

    /**
     * Returns a boolean indicating if the underlying rowIterator has a new element or not.
     * DOES NOT advance the iterator to the next element.
     *
     * @return
     */
    public boolean hasNext() {
	Iterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> it = getPrivateRowIterator();
	return it.hasNext();
    }

    /**
     * Returns the next element in the underlying rowIterator.
     *
     * @return
     */
    public Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> next(){
	if (!this.hasNext()){
	    throw new DeepIllegalAccessException("DeepCqlPagingRecordReader exhausted");
	}
	return getPrivateRowIterator().next();
    }
}
