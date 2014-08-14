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

import com.stratio.deep.extractor.action.Action;
import com.stratio.deep.extractor.action.ComputeAction;
import com.stratio.deep.extractor.action.GetPartitionsAction;

public class ExtractorServerHandler extends SimpleChannelInboundHandler<Action> {

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Action action) throws Exception {

    switch (action.getType()) {
      case COMPUTE:
        ComputeAction computeAction = (ComputeAction) action;
        break;
      case GET_PARTITIONS:
        GetPartitionsAction partitionsAction = (GetPartitionsAction) action;
        break;
      default:
        break;
    }

    ctx.write("Type: " + action.getType().name());
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
}
