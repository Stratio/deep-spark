/**
 * 
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.action.ActionType;

/**
 * @author Óscar Puertas
 * 
 */
public class GetPartitionsResponse extends Response {

  private static final long serialVersionUID = 9163365799147805458L;

  private int id;

  public GetPartitionsResponse() {
    super();
  }

  public GetPartitionsResponse(int id) {
    super(ActionType.GET_PARTITIONS);
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
