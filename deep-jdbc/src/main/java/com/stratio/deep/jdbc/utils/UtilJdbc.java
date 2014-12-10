package com.stratio.deep.jdbc.utils;

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.hadoop.DBRecord;
import scala.Tuple2;

/**
 * Created by mariomgal on 09/12/14.
 */
public class UtilJdbc {

    public static <T> T getObjectFromRow(Tuple2<Object, DBRecord> tuple, DeepJobConfig<T, JdbcDeepJobConfig<T>> config) {
        return null;
    }

    public static <T> Tuple2<Object, DBRecord> getRowFromObject(T entity) {
        return null;
    }

    public static Cells getCellsFromObject(Tuple2<Object, DBRecord> tuple, DeepJobConfig<Cells, JdbcDeepJobConfig<Cells>> config) {
        return null;
    }

    public static Tuple2<Object, DBRecord> getObjectFromCells(Cells cells) {
        return null;
    }
}
