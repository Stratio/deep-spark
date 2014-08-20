/**
 *
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.rdd.IDeepPartition;

/**
 * @author Óscar Puertas
 */
public class InitIteratorAction<T> extends Action {

    private static final long serialVersionUID = -1270097974102584045L;

    private DeepJobConfig<T> config;


    private IDeepPartition partition;

    public InitIteratorAction() {
        super();
    }

    public InitIteratorAction(IDeepPartition partition, DeepJobConfig<T> config) {
        super(ActionType.INIT_ITERATOR);
        this.config = config;
        this.partition = partition;
    }


    public DeepJobConfig<T> getConfig() {
        return config;
    }

    public IDeepPartition getPartition() {
        return partition;
    }
}
