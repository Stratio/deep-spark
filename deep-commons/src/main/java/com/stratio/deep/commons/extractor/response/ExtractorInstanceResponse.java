/**
 *
 */
package com.stratio.deep.commons.extractor.response;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.actions.ActionType;
import com.stratio.deep.commons.rdd.IExtractor;

/**
 * Created by rcrespo on 20/08/14.
 */
public class ExtractorInstanceResponse<T> extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

    private IExtractor<T, ExtractorConfig<T>> data;

    public ExtractorInstanceResponse(IExtractor<T, ExtractorConfig<T>> extractor) {
        super(ActionType.CLOSE);
        this.data = extractor;
    }

    public IExtractor<T, ExtractorConfig<T>> getData() {
        return data;
    }
}
