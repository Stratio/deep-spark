package com.stratio.deep.core.sql;

import com.stratio.deep.commons.entity.Cells;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import java.util.Iterator;

/**
 * Created by mariomgal on 26/11/14.
 */
public class DeepJavaSQLContext extends JavaSQLContext {


    public DeepJavaSQLContext(SQLContext sqlContext) {
        super(sqlContext);
    }

    public DeepJavaSQLContext(JavaSparkContext sparkContext) {
        super(sparkContext);
    }

    @Override
    public JavaSchemaRDD applySchema(JavaRDD<?> rdd, Class<?> beanClass) {
        JavaSchemaRDD result = null;
        if (Cells.class.isAssignableFrom(beanClass)) {

        } else {
            result = super.applySchema(rdd, beanClass);
        }
        return result;
    }

    @Override
    public JavaSchemaRDD sql(String sqlText) {
        return super.sql(sqlText);
    }


}
