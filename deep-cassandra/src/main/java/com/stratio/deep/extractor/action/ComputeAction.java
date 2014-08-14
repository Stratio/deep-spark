/**
 * 
 */
package com.stratio.deep.extractor.action;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import com.stratio.deep.config.IDeepJobConfig;

/**
 * @author Óscar Puertas
 * 
 */
public class ComputeAction<T> extends Action {

  private static final long serialVersionUID = -1270097974102584045L;

  private IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config;

  private Partition split;

  private TaskContext context;

  public ComputeAction() {
    super();
  }

  public ComputeAction(Partition split, TaskContext context,
      IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {
    super(ActionType.COMPUTE);
    this.split = split;
    this.context = context;
    this.config = config;
  }

  public Partition getSplit() {
    return split;
  }

  public TaskContext getContext() {
    return context;
  }

  public IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> getConfig() {
    return config;
  }

}
