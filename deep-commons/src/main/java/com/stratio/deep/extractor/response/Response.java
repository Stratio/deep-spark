/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

import java.io.Serializable;


/**
 * @author Óscar Puertas
 */
public abstract class Response implements Serializable {

    private static final long serialVersionUID = -3525560371269242119L;

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
