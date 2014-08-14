/**
 * 
 */
package com.stratio.deep.extractor.action;


/**
 * @author Óscar Puertas
 * 
 */
public class GetPartitionsAction extends Action {

  private static final long serialVersionUID = 9163365799147805458L;

  private int id;

  public GetPartitionsAction() {
    super();
  }

  public GetPartitionsAction(int id) {
    super(ActionType.GET_PARTITIONS);
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
