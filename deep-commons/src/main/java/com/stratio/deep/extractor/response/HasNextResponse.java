/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

/**
 * @author Óscar Puertas
 */
public class HasNextResponse<T> extends Response {

  private static final long serialVersionUID = -2647516898871636731L;

  private boolean hasNext;

  private T data;

  public HasNextResponse() {
    super();
  }

  public HasNextResponse(boolean hasNext, T data) {
    super(ActionType.HAS_NEXT);
    this.hasNext = hasNext;
    this.data = data;
  }

  public boolean getHasNext() {
    return hasNext;
  }

  public T getData() {
    return data;
  }
}
