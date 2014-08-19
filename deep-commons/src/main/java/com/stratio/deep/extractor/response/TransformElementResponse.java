/**
 * 
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

import java.util.Iterator;

/**
 * @author Óscar Puertas
 * 
 */
public class TransformElementResponse<T> extends Response {

  private static final long serialVersionUID = -2647516898871636731L;

  private T data;

  public TransformElementResponse() {
    super();
  }

  public TransformElementResponse(T data) {
    super(ActionType.TRANSFORM_ELEMENT);
    this.data = data;
  }

  public T getData() {
    return data;
  }
}
