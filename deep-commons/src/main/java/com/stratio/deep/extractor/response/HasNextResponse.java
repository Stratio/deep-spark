/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

/**
 * @author Óscar Puertas
 */
public class HasNextResponse extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

    private boolean data;

    public HasNextResponse() {
        super();
    }

    public HasNextResponse(boolean data) {
        super(ActionType.HAS_NEXT);
        this.data = data;
    }

    public boolean getData() {
        return data;
    }
}
