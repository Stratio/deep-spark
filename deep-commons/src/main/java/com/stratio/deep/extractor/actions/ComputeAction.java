/**
 *
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.extractor.core.IDeepRecordReader;
import com.stratio.deep.utils.Pair;
import org.apache.spark.TaskContext;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author Óscar Puertas
 */
public class ComputeAction<T> extends Action {

    private static final long serialVersionUID = -1270097974102584045L;

    private IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config;

    private IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader;

    private TaskContext context;

    public ComputeAction() {
        super();
    }

    public ComputeAction(IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader,
                         IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {
        super(ActionType.COMPUTE);
        this.recordReader = recordReader;
        this.config = config;
    }

    public IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> getRecordReader() {
        return recordReader;
    }

    public TaskContext getContext() {
        return context;
    }

    public IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> getConfig() {
        return config;
    }

}
