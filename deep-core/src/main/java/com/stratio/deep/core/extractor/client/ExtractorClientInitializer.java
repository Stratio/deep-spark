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
    registerKryoResponse(kryo);

    Kryo kryo2 = new Kryo();
    registerKryoAction(kryo2);


    p.addLast(new ResponseDecoder(kryo));
    p.addLast(new ActionEncoder(kryo2, 4 * 1024, 16 * 1024));

    p.addLast(new ExtractorClientHandler<T>());
  }


  private void registerKryoAction(Kryo kryo) {

    registerAction(kryo);
  }

  private void registerKryoResponse(Kryo kryo) {
    registerResponse(kryo);
    registerUtils(kryo);
  }

  private void registerAction(Kryo kryo) {
    kryo.register(HasNextAction.class);
    kryo.register(GetPartitionsAction.class);
    kryo.register(ExtractorInstanceAction.class);
    kryo.register(InitIteratorAction.class);
    kryo.register(InitSaveAction.class);
    kryo.register(SaveAction.class);
    kryo.register(CloseAction.class);
  }

  private void registerResponse(Kryo kryo) {
    kryo.register(HasNextResponse.class);
    kryo.register(LinkedList.class);
    kryo.register(HasNextElement.class);
    kryo.register(GetPartitionsResponse.class);
    kryo.register(ExtractorInstanceResponse.class);
    kryo.register(InitIteratorResponse.class);
    kryo.register(InitSaveResponse.class);
    kryo.register(SaveResponse.class);
  }

  private void registerUtils(Kryo kryo) {
    kryo.register(UUID.class, new UUIDSerializer());
  }



}
