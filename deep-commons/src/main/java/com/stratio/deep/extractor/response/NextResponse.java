/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

/**
 * @author Óscar Puertas
 */
public class NextResponse<T> extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

    private T data;

    public NextResponse() {
        super();
    }

    public NextResponse(T data) {
        super(ActionType.NEXT);
        this.data = data;
    }

    public T getData() {
        return data;
    }
}
