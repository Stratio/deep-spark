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
package com.stratio.deep.core.extractor.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.stratio.deep.extractor.actions.*;
import com.stratio.deep.extractor.client.codecs.ActionEncoder;
import com.stratio.deep.extractor.client.codecs.ResponseDecoder;
import com.stratio.deep.extractor.response.*;

import com.stratio.deep.extractor.utils.KryoRegister;
import de.javakaffee.kryoserializers.UUIDSerializer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import java.util.LinkedList;
import java.util.UUID;

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

      Kryo kryo = new Kryo();
      KryoRegister.registerResponse(kryo);

      Kryo kryo2 = new Kryo();
      KryoRegister.registerAction(kryo2);


    p.addLast(new ResponseDecoder(kryo));
    p.addLast(new ActionEncoder(kryo2, 16 * 1024,-1));

    p.addLast(new ExtractorClientHandler<T>());
  }



}
