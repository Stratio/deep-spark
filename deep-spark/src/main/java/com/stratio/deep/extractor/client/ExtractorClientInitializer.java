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
package com.stratio.deep.extractor.client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import java.io.Serializable;

import com.stratio.deep.extractor.client.codecs.ActionEncoder;
import com.stratio.deep.extractor.client.codecs.ResponseDecoder;

public class ExtractorClientInitializer<T> extends ChannelInitializer<SocketChannel> {

  private final SslContext sslCtx;

  public ExtractorClientInitializer(SslContext sslCtx) {
    this.sslCtx = sslCtx;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    ChannelPipeline p = ch.pipeline();
    if (sslCtx != null) {
      p.addLast(sslCtx.newHandler(ch.alloc(), ExtractorClient.HOST, ExtractorClient.PORT));
    }

    p.addLast(new ResponseDecoder());
    p.addLast(new ActionEncoder());

    p.addLast(new ExtractorClientHandler<T>());
  }
}
