/**
 *
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.DeepJobConfig;

public class GetPartitionsAction<T> extends Action {

    private static final long serialVersionUID = 9163365799147805458L;

    private DeepJobConfig<T> config;

    public GetPartitionsAction() {
        super();
    }

    public GetPartitionsAction(DeepJobConfig<T> config) {
        super(ActionType.GET_PARTITIONS);
        this.config = config;
    }

    public DeepJobConfig<T> getConfig() {
        return config;
    }


}
