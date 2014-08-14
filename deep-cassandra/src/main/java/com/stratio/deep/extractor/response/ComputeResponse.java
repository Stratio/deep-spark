/**
 * 
 */
package com.stratio.deep.extractor.response;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import com.stratio.deep.extractor.action.ActionType;

/**
 * @author Óscar Puertas
 * 
 */
public class ComputeResponse extends Response {

  private static final long serialVersionUID = -1270097974102584045L;

  private Partition split;

  private TaskContext context;

  public ComputeResponse() {
    super();
  }

  public ComputeResponse(Partition split, TaskContext context) {
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
