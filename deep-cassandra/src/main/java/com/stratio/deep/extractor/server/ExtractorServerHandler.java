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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.spark.Partition;

import java.util.Iterator;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.extractor.actions.Action;
import com.stratio.deep.extractor.actions.ComputeAction;
import com.stratio.deep.extractor.actions.GetPartitionsAction;
import com.stratio.deep.extractor.response.ComputeResponse;
import com.stratio.deep.extractor.response.GetPartitionsResponse;
import com.stratio.deep.extractor.response.Response;
import com.stratio.deep.rdd.CassandraRDD;

public class ExtractorServerHandler<T> extends SimpleChannelInboundHandler<Action> {

  private CassandraRDD<T> extractor;
  
  @Override
  public void channelRead0(ChannelHandlerContext ctx, Action action) throws Exception {

    Response response = null;
    switch (action.getType()) {
      case COMPUTE:
        ComputeAction<T> computeAction = (ComputeAction<T>) action;
        response = new ComputeResponse(this.compute(computeAction));
        break;
      case GET_PARTITIONS:
        GetPartitionsAction<T> partitionsAction = (GetPartitionsAction<T>) action;
        response = new GetPartitionsResponse(this.getPartitions(partitionsAction));
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
  
  protected Iterator<T> compute(ComputeAction<T> computeAction) {
    
    if (extractor == null) {
      this.initExtractor(computeAction.getConfig());
    }
    
    return extractor.compute(computeAction.getRecordReader(), computeAction.getConfig());
    
  }
  
  protected Partition[] getPartitions(GetPartitionsAction<T> getPartitionsAction){
    
    if (extractor == null) {
      this.initExtractor(getPartitionsAction.getConfig());
    }

    return extractor.getPartitions(getPartitionsAction.getConfig(), getPartitionsAction.getId());
  }

  /**
   * @param config
   */
  @SuppressWarnings("unchecked")
  private void initExtractor(IDeepJobConfig<T, ? extends IDeepJobConfig<?, ?>> config) {

    Class<T> rdd = (Class<T>) config.getRDDClass();
    try {
    final Constructor<T> c = rdd.getConstructor();
      this.extractor = (CassandraRDD<T>) c.newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | NoSuchMethodException | SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
