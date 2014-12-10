package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.hadoop.DBRecord;
import com.stratio.deep.jdbc.utils.UtilJdbc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcCellExtractor extends JdbcExtractor<Cells>{

    private static final long serialVersionUID = -3489013875097417330L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCellExtractor.class);

    public JdbcCellExtractor(Class entityClass) {
        super();
        this.deepJobConfig = new JdbcDeepJobConfig(entityClass);
    }

    public JdbcCellExtractor() {
        super();
        this.deepJobConfig = new JdbcDeepJobConfig(Cells.class);
    }

    @Override
    public Tuple2<Object, DBRecord> transformElement(Cells cells) {
        return UtilJdbc.getObjectFromCells(cells);
    }

    @Override
    public Cells transformElement(Tuple2<Object, DBRecord> tuple, DeepJobConfig<Cells, JdbcDeepJobConfig<Cells>> config) {
        return UtilJdbc.getCellsFromObject(tuple, config);
    }
}
