package com.stratio.deep.entity;

import java.io.Serializable;

public interface DeepByteBuffer<F> {

    public abstract String getFieldName();

    public abstract Serializable getObject();

    public abstract Class<F> getType();

}