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

import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcWriter<T> {

    private JdbcDeepJobConfig<T> jdbcDeepJobConfig;

    private Connection conn;

    public JdbcWriter(JdbcDeepJobConfig jdbcDeepJobConfig) throws Exception {
        this.jdbcDeepJobConfig = jdbcDeepJobConfig;
        Class.forName(jdbcDeepJobConfig.getDriverClass());
        this.conn = DriverManager.getConnection(jdbcDeepJobConfig.getJdbcUrl(),
                jdbcDeepJobConfig.getUsername(),
                jdbcDeepJobConfig.getPassword());
    }

    public void save(Map<String, Object> row) throws SQLException {
        PreparedStatement statement = conn.prepareStatement(sqlFromRow(row));
        int i = 1;
        for(Object value:row.values()) {
            statement.setObject(i, value);
            i++;
        }
        statement.executeUpdate();
    }

    public void close() throws SQLException {
        conn.close();
    }

    private String sqlFromRow(Map<String, Object> row) {
        List<String> params = new ArrayList<>();
        for(int i=0; i<row.size(); i++) {
            params.add("?");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(jdbcDeepJobConfig.getTable());
        sb.append(" (");
        sb.append(StringUtils.join(row.keySet(), ","));
        sb.append(" ) VALUES (");
        sb.append(StringUtils.join(params, ","));
        sb.append(")");
        return sb.toString();
    }



}
