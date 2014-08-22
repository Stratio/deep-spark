/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;
import com.stratio.deep.rdd.DeepTokenRange;

/**
 * @author Óscar Puertas
 */
public class GetPartitionsResponse extends Response {

    private static final long serialVersionUID = -7728817078374511478L;

    private DeepTokenRange[] partitions;

    public GetPartitionsResponse() {
        super();
    }

    public GetPartitionsResponse(DeepTokenRange[] partitions) {
        super(ActionType.GET_PARTITIONS);
        this.partitions = partitions;
    }

    public DeepTokenRange[] getPartitions() {
        return partitions;
    }
}
