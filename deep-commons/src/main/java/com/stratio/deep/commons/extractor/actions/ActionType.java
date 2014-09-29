/**
 *
 */
package com.stratio.deep.commons.extractor.actions;

/**
 * @author Óscar Puertas
 */
public enum ActionType {

    CLOSE(1), GET_PARTITIONS(2), GET_PREFERRED(3), SAVE(4), EXTRACTOR_INSTANCE(5), HAS_NEXT(6), NEXT(7), INIT_ITERATOR(
            8), INIT_SAVE(9);

    private final int actionId;

    ActionType(int actionId) {
        this.actionId = actionId;
    }

    public int getActionId() {
        return actionId;
    }
}
