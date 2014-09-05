/**
 *
 */
package com.stratio.deep.extractor.response;

import java.util.LinkedList;

import com.stratio.deep.extractor.actions.ActionType;

/**
 * @author Óscar Puertas
 */
public class HasNextResponse<T> extends Response {

  private static final long serialVersionUID = -2647516898871636731L;

  private LinkedList<HasNextElement<T>> hasNextElementsPage;

  public HasNextResponse() {
    super();
  }

  public HasNextResponse(LinkedList<HasNextElement<T>> hasNextElementsPage) {
    super(ActionType.HAS_NEXT);
    this.hasNextElementsPage = hasNextElementsPage;
  }

  public LinkedList<HasNextElement<T>> getHasNextElementsPage() {
    return hasNextElementsPage;
  }
}
