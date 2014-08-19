/**
 * 
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.IDeepJobConfig;

public class GetPartitionsAction<T> extends Action {

  private static final long serialVersionUID = 9163365799147805458L;

  private IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config;

  private int id;

  public GetPartitionsAction() {
    super();
  }

  public GetPartitionsAction(IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config, int id) {
    super(ActionType.GET_PARTITIONS);
    this.config = config;
    this.id = id;
  }

  public IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> getConfig() {
    return config;
  }

  public int getId() {
    return id;
  }
}
