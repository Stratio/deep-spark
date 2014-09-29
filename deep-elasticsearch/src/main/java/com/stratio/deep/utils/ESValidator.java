package com.stratio.deep.utils;

import java.util.Map;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.ImmutableMap;

/**
 * Created by dgomez on 2/09/14.
 */
public class ESValidator {

    public static final Map<Class, Class<? extends Writable>> MAP_JAVA_TYPE_TO_WRITABLE_TYPE =
            ImmutableMap.<Class, Class<? extends Writable>>builder()
                    .put(String.class, Text.class)
                    .put(Integer.class, IntWritable.class)
                    .put(Boolean.class, BooleanWritable.class)
                    .put(Long.class, LongWritable.class)
                    .put(Double.class, DoubleWritable.class)
                    .put(Float.class, FloatWritable.class)
                    .build();

    public static final Map<Class<? extends Writable>, Class> MAP_WRITABLE_TYPE_TO_JAVA_TYPE =
            ImmutableMap.<Class<? extends Writable>, Class>builder()
                    .put(Text.class, String.class)
                    .put(IntWritable.class, Integer.class)
                    .put(BooleanWritable.class, Boolean.class)

                    .put(LongWritable.class, Long.class)
                    .put(DoubleWritable.class, Double.class)
                    .put(FloatWritable.class, Float.class)
                    .build();

    /**
     * private constructor.
     */
    private static Class<? extends Writable> getCollectionInnerType(Class<?> type) {
        Class<? extends Writable> writableType = MAP_JAVA_TYPE_TO_WRITABLE_TYPE.get(type);
        return writableType;
    }

}
