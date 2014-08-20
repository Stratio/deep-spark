/**
 *
 */
package com.stratio.deep.extractor.actions;

/**
 * @author Óscar Puertas
 */
public class HasNextAction<T> extends Action {

    private static final long serialVersionUID = -1270097974102584045L;

//  private DeepJobConfig<T> config;
//
//  private IDeepRecordReader<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> recordReader;
//
//  private TaskContext context;
//
//    private IDeepPartition partition;

//  public HasNextAction() {
//    super();
//  }

    public HasNextAction() {
        super(ActionType.HAS_NEXT);
    }
}
