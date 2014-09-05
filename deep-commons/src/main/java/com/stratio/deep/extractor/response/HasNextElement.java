/**
 *
 */
package com.stratio.deep.extractor.response;

import java.io.Serializable;

/**
 * @author Óscar Puertas
 */
public class HasNextElement<T> implements Serializable {

  private static final long serialVersionUID = -8010970807991404423L;

  private boolean hasNext;

  private T data;

  public HasNextElement() {
  }

  public HasNextElement(boolean hasNext, T data) {
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
