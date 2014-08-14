/**
 * 
 */
package com.stratio.deep.extractor.response;

import scala.collection.Iterator;

import com.stratio.deep.extractor.action.ActionType;

/**
 * @author Óscar Puertas
 * 
 */
public class ComputeResponse<T> extends Response {

  private static final long serialVersionUID = -1270097974102584045L;

  private Iterator<T> data;

  public ComputeResponse() {
    super();
  }

  public ComputeResponse(Iterator<T> data) {
    super(ActionType.COMPUTE);
    this.data = data;
  }

  public Iterator<T> getData() {
    return data;
  }
}
