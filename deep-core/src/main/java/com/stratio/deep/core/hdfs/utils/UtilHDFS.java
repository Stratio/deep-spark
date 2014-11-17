package com.stratio.deep.core.hdfs.utils;

import java.lang.reflect.InvocationTargetException;

import com.stratio.deep.commons.entity.Cells;

public class UtilHDFS {

    public static Cells getCellFromLine(String line, String tableName) throws IllegalAccessException,
            InstantiationException, InvocationTargetException {

        Cells cells = tableName!= null ?new Cells(tableName): new Cells();




        return cells;
    }
}
