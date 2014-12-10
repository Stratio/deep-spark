package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.hadoop.DBRecord;
import com.stratio.deep.jdbc.utils.UtilJdbc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by mariomgal on 09/12/14.
 */
public class JdbcEntityExtractor<T> extends JdbcExtractor<T> {

    private static final long serialVersionUID = 940383901994404978L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExtractor.class);

    public JdbcEntityExtractor(Class<T> t) {
        super();
        this.deepJobConfig = new JdbcDeepJobConfig(t);
    }

    @Override
    public T transformElement(Tuple2<Object, DBRecord> tuple, DeepJobConfig<T, JdbcDeepJobConfig<T>> config) {
        return UtilJdbc.getObjectFromRow(tuple, config);
    }

    @Override
    public Tuple2<Object, DBRecord> transformElement(T entity) {
        return UtilJdbc.getRowFromObject(entity);
    }
}
