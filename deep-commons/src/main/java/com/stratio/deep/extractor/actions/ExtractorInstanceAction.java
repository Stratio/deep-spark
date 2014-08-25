/**
 *
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.ExtractorConfig;

/**
 * Created by rcrespo on 20/08/14.
 */
public class ExtractorInstanceAction<T> extends Action {

    private static final long serialVersionUID = -1270097974102584045L;

    private ExtractorConfig<T> config;

    public ExtractorInstanceAction(ExtractorConfig<T> config) {
        super(ActionType.EXTRACTOR_INSTANCE);
    }

    public ExtractorConfig<T> getConfig() {
        return config;
    }
}
