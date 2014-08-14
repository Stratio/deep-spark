/**
 * 
 */
package com.stratio.deep.extractor.response;

import java.io.Serializable;

import com.stratio.deep.extractor.action.ActionType;


/**
 * @author Óscar Puertas
 * 
 */
public abstract class Response implements Serializable {

  private static final long serialVersionUID = -2701732752347654671L;

  protected ActionType type;

  protected Response() {
    super();
  }

  public Response(ActionType type) {
    super();
    this.type = type;
  }

  public ActionType getType() {
    return type;
  }
}
