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
package com.stratio.deep.jdbc.reader;

import org.apache.spark.Partition;

import java.sql.SQLException;
import java.util.Map;

/**
 * Interface for JDBC Reader helpers.
 */
public interface IJdbcReader extends AutoCloseable {

    /**
     * Inits the JDBC Reader helper with a given Spark partition.
     * @param p Spark partition.
     * @throws Exception
     */
    void init(Partition p) throws Exception;

    /**
     * Checks if there are more elements for reading.
     * @return True if there are pending elements for reading.
     * @throws SQLException
     */
    boolean hasNext() throws SQLException;

    /**
     * Fetches the next object.
     * @return Next object in reader.
     * @throws SQLException
     */
    Map<String, Object> next() throws SQLException;

}
