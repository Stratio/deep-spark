/**
 * 
 */
package com.stratio.deep.extractor.action;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

/**
 * @author Óscar Puertas
 * 
 */
public class ComputeAction extends Action {

  private static final long serialVersionUID = -1270097974102584045L;

  private Partition split;

  private TaskContext context;

  public ComputeAction() {
    super();
  }

  public ComputeAction(Partition split, TaskContext context) {
    super(ActionType.COMPUTE);
    this.split = split;
    this.context = context;
  }

  public Partition getSplit() {
    return split;
  }

  public TaskContext getContext() {
    return context;
  }

}
