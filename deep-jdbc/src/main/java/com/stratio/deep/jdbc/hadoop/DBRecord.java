package com.stratio.deep.jdbc.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class DBRecord implements Writable, DBWritable {

    private Map<String, Object> columns = new HashMap<>();

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnsNumber = metadata.getColumnCount();
        for(int i=0; i<columnsNumber; i++) {
            String columnName = metadata.getColumnName(i);
            columns.put(columnName, resultSet.getObject(i));
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    public Map<String, Object> columns() {
        return columns;
    }

}
