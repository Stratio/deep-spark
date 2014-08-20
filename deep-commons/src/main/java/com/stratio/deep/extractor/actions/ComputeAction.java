/**
 * 
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.rdd.IDeepPartition;
import com.stratio.deep.rdd.IDeepRecordReader;
import com.stratio.deep.utils.Pair;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import com.stratio.deep.config.IDeepJobConfig;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author Óscar Puertas
 * 
 */
public class ComputeAction<T> extends Action {

  private static final long serialVersionUID = -1270097974102584045L;

  private DeepJobConfig<T> config;

  private IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader;

  private TaskContext context;

    private IDeepPartition partition;

  public ComputeAction() {
    super();
  }

  public ComputeAction(TaskContext context, IDeepPartition partition, DeepJobConfig<T> config) {
    super(ActionType.COMPUTE);
    this.context = context;
    this.config = config;
      this.partition=partition;
  }

    public IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> getRecordReader() {
        return recordReader;
    }

    public TaskContext getContext() {
    return context;
  }

  public DeepJobConfig<T> getConfig() {
    return config;
  }

    public IDeepPartition getPartition() {
        return partition;
    }
}
