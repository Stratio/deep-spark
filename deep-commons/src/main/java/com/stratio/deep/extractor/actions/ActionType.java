/**
 * 
 */
package com.stratio.deep.extractor.actions;


/**
 * @author Óscar Puertas
 * 
 */
public enum ActionType {

  COMPUTE(1), GET_PARTITIONS(2), GET_PREFERRED(3), WRITE(4), TRANSFORM_ELEMENT(5), HAS_NEXT(6), NEXT(7), INIT_ITERATOR(8);

  private final int actionId;

  ActionType(int actionId) {
    this.actionId = actionId;
  }

  public int getActionId() {
    return actionId;
  }
}
