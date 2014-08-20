/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

/**
 * @author Óscar Puertas
 */
public class InitIteratorResponse<T> extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

    private boolean data;

//  public InitIteratorResponse() {
//    super();
//  }

    public InitIteratorResponse() {
        super(ActionType.INIT_ITERATOR);
        this.data = true;
    }

    public boolean getData() {
        return data;
    }
}
