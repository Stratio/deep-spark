/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

/**
 * Created by rcrespo on 20/08/14.
 */
public class CloseResponse extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

    private boolean data;


    public CloseResponse() {
        super(ActionType.CLOSE);
        this.data = true;
    }

    public boolean getData() {
        return data;
    }
}
