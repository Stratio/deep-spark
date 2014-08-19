/**
 * 
 */
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.rdd.IDeepRecordReader;
import com.stratio.deep.utils.Pair;
import org.apache.spark.TaskContext;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author Óscar Puertas
 * 
 */
public class TransformElementAction<T> extends Action {

  private static final long serialVersionUID = -1270097974102584045L;

  private IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config;

  private Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> element;

  public TransformElementAction() {
    super();
  }

  public TransformElementAction(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> element,
                                IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {
    super(ActionType.TRANSFORM_ELEMENT);
    this.element = element;
    this.config = config;
  }

    public Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> getElement() {
        return element;
    }

    public IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> getConfig() {
    return config;
  }

}
