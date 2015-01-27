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
package com.stratio.deep.jdbc.writer;

import java.sql.SQLException;
import java.util.Map;

/**
 * Interface for JDBC Writer Helpers.
 */
public interface IJdbcWriter extends AutoCloseable {

    /**
     * Saves a row using a JDBC connection.
      * @param row Row to save.
     * @throws Exception
     */
    void save(Map<String, Object> row) throws Exception;

}
