package com.stratio.deep.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DeepField {

    String fieldName() default "";

    boolean isPartOfClusterKey() default false;

    boolean isPartOfPartitionKey() default false;

    Class<? extends AbstractType<?>> validationClass() default UTF8Type.class;

}
