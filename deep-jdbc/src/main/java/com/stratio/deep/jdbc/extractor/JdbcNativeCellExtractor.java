package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.utils.UtilJdbc;

import javax.rmi.CORBA.Util;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcNativeCellExtractor extends JdbcNativeExtractor<Cells, JdbcDeepJobConfig<Cells>> {

    private static final long serialVersionUID = 5796562363902015583L;

    public JdbcNativeCellExtractor() {
        this.jdbcDeepJobConfig = new JdbcDeepJobConfig<>(Cells.class);
    }

    public JdbcNativeCellExtractor(Class<Cells> cellsClass) {
        this.jdbcDeepJobConfig = new JdbcDeepJobConfig<>(Cells.class);
    }

    @Override
    protected Cells transformElement(Map<String, Object> entity) {
        return UtilJdbc.getCellsFromObject(entity, jdbcDeepJobConfig);
    }

    @Override
    protected Map<String, Object> transformElement(Cells cells) {
        return UtilJdbc.getObjectFromCells(cells);
    }

}
