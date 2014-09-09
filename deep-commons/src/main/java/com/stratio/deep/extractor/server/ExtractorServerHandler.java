/*
 * Copyright 2012 The Netty Project
 * 
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.stratio.deep.extractor.server;

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.extractor.actions.*;
import com.stratio.deep.extractor.response.*;
import com.stratio.deep.rdd.IExtractor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.spark.Partition;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;

public class ExtractorServerHandler<T> extends SimpleChannelInboundHandler<Action> {

  // Max number of elements per page to return to the HasNextAction
  private final static int MAX_ELEMENTS_PER_PAGE_CONSTANT = 5000;

  private IExtractor<T> extractor;

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Action action) throws Exception {

    Response response = null;

    switch (action.getType()) {
      case GET_PARTITIONS:
        GetPartitionsAction<T> partitionsAction = (GetPartitionsAction<T>) action;
        response = new GetPartitionsResponse(this.getPartitions(partitionsAction));
        break;
      case CLOSE:
        this.close();
        response = new CloseResponse();
        break;
      case HAS_NEXT:
        HasNextAction<T> hasNextAction = (HasNextAction<T>) action;
        int numElements = 0;
        LinkedList<HasNextElement<T>> hasNextElementsPage = new LinkedList<>();
          while (this.hasNext() && numElements < MAX_ELEMENTS_PER_PAGE_CONSTANT) {
          T data =this.next();
          hasNextElementsPage.add(new HasNextElement<>(this.hasNext(), data));
          numElements++;
        }
        response = new HasNextResponse<T>(hasNextElementsPage);
        break;
      case INIT_ITERATOR:
        InitIteratorAction<T> initIteratorAction = (InitIteratorAction<T>) action;
        this.initIterator(initIteratorAction);
        response = new InitIteratorResponse();
        break;
      case SAVE:
        SaveAction<T> SaveAction = (SaveAction<T>) action;
        this.save(SaveAction);
        response = new SaveResponse();
        break;
      case INIT_SAVE:
        InitSaveAction<T> initSave = (InitSaveAction<T>) action;
        this.initSave(initSave);
        response = new InitSaveResponse();
        break;
      default:
        break;
    }

    ctx.write(response);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }


  protected boolean hasNext() {

    return extractor.hasNext();

  }

  protected T next() {

    return extractor.hasNext() ? extractor.next() : null;

  }

  protected void close() {
    extractor.close();
    return;

  }

  protected void initIterator(InitIteratorAction<T> initIteratorAction) {
    if (extractor == null) {
      this.initExtractor(initIteratorAction.getConfig());
    }

    extractor.initIterator(initIteratorAction.getPartition(), initIteratorAction.getConfig());
    return;

  }

  protected Partition[] getPartitions(GetPartitionsAction<T> getPartitionsAction) {

    if (extractor == null) {
      this.initExtractor(getPartitionsAction.getConfig());
    }

    return extractor.getPartitions(getPartitionsAction.getConfig());
  }


  /**
   * @param config
   */
  @SuppressWarnings("unchecked")
  private void initExtractor(ExtractorConfig<T> config) {

    Class<T> rdd = (Class<T>) config.getExtractorImplClass();
    try {
      Constructor<T> c = null;
      if (config.getEntityClass().isAssignableFrom(Cells.class)) {
        c = rdd.getConstructor();
        this.extractor = (IExtractor<T>) c.newInstance();
      } else {
        c = rdd.getConstructor(Class.class);
        this.extractor = (IExtractor<T>) c.newInstance(config.getEntityClass());
      }


    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | NoSuchMethodException | SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  protected void initSave(InitSaveAction<T> initSaveAction) {
    if (extractor == null) {
      this.initExtractor(initSaveAction.getConfig());
    }

    extractor.initSave(initSaveAction.getConfig(), initSaveAction.getFirst());
    return;

  }

  protected void save(SaveAction<T> saveAction) {
    extractor.saveRDD(saveAction.getRecord());
    return;

  }
}
