package com.stratio.deep.core.hdfs.utils;

import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.utils.Utils;

public class MapSchemaFromLines implements Function<String, Cells> {

    /**
     * Columns
     */
    private final List<SchemaMap<?>> columns;

    /**
     * Separator in the HDFS FIle
     */
    private String separator;

    /**
     * TableName of the HDFS FIle
     */
    private String tableName;

    /**
     * Catalog of the HDFS FIle
     */
    private String catalogName;

    public MapSchemaFromLines(TextFileDataTable textFileDataTable) {
        this.tableName = textFileDataTable.getTableName().getTableName();
        this.catalogName = textFileDataTable.getTableName().getCatalogName();
        this.columns = textFileDataTable.getColumnMap();
        this.separator = textFileDataTable.getLineSeparator();
    }

    @Override
    public Cells call(String s) throws Exception {

        Cells cellsOut = new Cells(catalogName + "." + tableName);

        String[] splits = s.split(separator);

        int i = 0;
        for (SchemaMap schemaMap : columns) {

            Cell cell = Cell.create(schemaMap.getFieldName(), Utils.castingUtil(splits[i], schemaMap.getFieldType()));
            cellsOut.add(cell);
            i++;
        }

        return cellsOut;
    }

}
