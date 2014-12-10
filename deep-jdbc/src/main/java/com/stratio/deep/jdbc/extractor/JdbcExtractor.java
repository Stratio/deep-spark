package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.extractor.impl.GenericHadoopExtractor;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.hadoop.DBRecord;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

import java.io.Serializable;

/**
 * Created by mariomgal on 09/12/14.
 */
public abstract class JdbcExtractor<T> extends GenericHadoopExtractor<T, JdbcDeepJobConfig<T>, Object,
        DBRecord, Object, DBRecord>
        implements Serializable {

    private static final long serialVersionUID = -8715542794052408133L;

    public JdbcExtractor() {
        this.inputFormat = new DBInputFormat();
        this.outputFormat = new DBOutputFormat();
    }

}
