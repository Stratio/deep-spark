/**
 *
 */
package com.stratio.deep.extractor.actions;

import java.io.Serializable;

/**
 * @author Óscar Puertas
 */
public class Action implements Serializable {

    private static final long serialVersionUID = -2701732752347654671L;

    protected ActionType type;

    protected Action() {
        super();
    }

    protected Action(ActionType type) {
        super();
        this.type = type;
    }

    public ActionType getType() {
        return type;
    }
}
