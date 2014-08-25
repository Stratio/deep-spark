/**
 *
 */
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;
import com.stratio.deep.rdd.DeepTokenRange;
import org.apache.spark.Partition;

/**
 * @author Óscar Puertas
 */
public class GetPartitionsResponse extends Response {

    private static final long serialVersionUID = -7728817078374511478L;

    private Partition[] partitions;

    public GetPartitionsResponse() {
        super();
    }

    public GetPartitionsResponse(Partition[] partitions) {
        super(ActionType.GET_PARTITIONS);
        this.partitions = partitions;
    }

    public Partition[] getPartitions() {
        return partitions;
    }
}
