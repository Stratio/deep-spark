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

package com.stratio.deep.annotations;

import org.apache.cassandra.db.marshal.AbstractType;
//import org.apache.cassandra.db.marshal.UTF8Type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Maps an object property to a Database column.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DeepField {

    /**
     * used to specify an alternative database name for the current property.
     * If omitted the property name will be used to resolve the database column name.
     */
    String fieldName() default "";

    /**
     * Specifies if the current field is part of the clustering key. Defaults to false.
     */
    boolean isPartOfClusterKey() default false;

    /**
     * Specifies if the current field is part of the partitioning key. Defaults to false.
     */
    boolean isPartOfPartitionKey() default false;

    /**
     * Specifies the cassandra validator class to be used to marshall/unmarshall the field value to the database.
     * Defaults to org.apache.cassandra.db.marshal.UTF8Type.class
     */
    Class<? extends AbstractType> validationClass();

}
