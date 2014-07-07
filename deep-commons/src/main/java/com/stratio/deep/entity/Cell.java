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

package com.stratio.deep.entity;

import com.datastax.driver.core.DataType;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepInstantiationException;
import org.apache.cassandra.db.marshal.AbstractType;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static com.stratio.deep.utils.AnnotationUtils.*;

/**
 * Generic abstraction for cassandra's columns.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public interface Cell extends Serializable {

    /**
     * @return the name of the current cell as defined in the database.
     */
    public String getCellName();

    /**
     * Returns the composed cell value. The type of the returned object can be obtained by calling
     * {@link com.stratio.deep.entity.Cell}
     *
     * @return the composed cell value.
     */
    public Object getCellValue();

    /**
     * @return Returns the validator object associated to this Cell.
     */
    public CellValidator getCellValidator();

    /**
     * Returns the Java type corresponding to the current Cell.
     *
     * @return the Java type corresponding to the current Cell.
     */
    public Class<?> getValueType();

    /**
     * Returns the cell value as a ByteBuffer, performs the conversion using the
     * configured validator.
     * <p/>
     * If cell value is null we propagate an empty array, see CASSANDRA-5885 and CASSANDRA-6180.
     *
     * @return Returns the cell value as a ByteBuffer.
     */
    public ByteBuffer getDecomposedCellValue();

    /**
     * @return true if the current cell is part of the clustering key.
     */
    public Boolean isClusterKey();

    /**
     * @return true if the current cell is part of the partition key.
     */

    public Boolean isPartitionKey();

    /**
     * @return Returns the validator instance associated to this cell.
     */
    public AbstractType marshaller();

    /**
     * @return the fully qualified class name of the validator associated to this cell.
     */
    public String marshallerClassName();

}
