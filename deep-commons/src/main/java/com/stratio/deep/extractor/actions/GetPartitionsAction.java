/**
 *
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.ExtractorConfig;

public class GetPartitionsAction<T> extends Action {

    private static final long serialVersionUID = 9163365799147805458L;

    private ExtractorConfig<T> config;

    public GetPartitionsAction() {
        super();
    }

    public GetPartitionsAction(ExtractorConfig<T> config) {
        super(ActionType.GET_PARTITIONS);
        this.config = config;
    }

    public ExtractorConfig<T> getConfig() {
        return config;
    }


}
