/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

import java.util.Iterator;

/**
 * @author Óscar Puertas
 */
public class ComputeResponse<T> extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

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
