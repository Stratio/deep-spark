/**
 * 
 */
package com.stratio.deep.extractor.actions;


/**
 * @author Óscar Puertas
 * 
 */
public enum ActionType {

  COMPUTE(1), GET_PARTITIONS(2), GET_PREFERRED(3), WRITE(4);

  private final int actionId;

  ActionType(int actionId) {
    this.actionId = actionId;
  }

  public int getActionId() {
    return actionId;
  }
}
