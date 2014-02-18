package com.stratio.deep.entity.impl;

import java.io.Serializable;

import com.stratio.deep.entity.DeepByteBuffer;
import com.stratio.deep.entity.IDeepType;

/**
 * Wrapper for {@link IDeepType} serializable fields.
 * 
 * @author Luca Rosellini <luca@strat.io>
 *
 * @param <F>
 */
public final class DeepByteBufferImpl<F> implements Serializable, DeepByteBuffer<F> {

    private static final long serialVersionUID = 4487608627926403668L;

    /**
     * Creates a new {@link DeepByteBufferImpl}.
     * 
     * @param fieldName
     * @param type
     * @param object
     * @return
     */
    public static <F> DeepByteBuffer<F> create(String fieldName, Class<F> type, Serializable object) {
	return new DeepByteBufferImpl<>(fieldName, object, type);
    }

    /**
     * Type of the serializable field.
     */
    private final Class<F> type;

    /**
     * The actual object representing the field.
     */
    private final Serializable object;

    /**
     * The name of the field inside {@link IDeepType}.
     */
    private final String fieldName;

    /**
     * private constructor.
     * 
     * @param fieldName
     * @param object
     * @param type
     */
    private DeepByteBufferImpl(String fieldName, Serializable object, Class<F> type) {
	super();
	this.object = object;
	this.type = type;
	this.fieldName = fieldName;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.entity.impl.DeepByteBuffer#getFieldName()
     */
    @Override
    public String getFieldName() {
	return fieldName;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.entity.impl.DeepByteBuffer#getObject()
     */
    @Override
    public Serializable getObject() {
	return object;
    }

    /* (non-Javadoc)
     * @see com.stratio.deep.entity.impl.DeepByteBuffer#getType()
     */
    @Override
    public Class<F> getType() {
	return type;
    }
}
