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

import com.stratio.deep.extractor.utils.KryoRegister;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import java.util.LinkedList;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.stratio.deep.extractor.actions.CloseAction;
import com.stratio.deep.extractor.actions.ExtractorInstanceAction;
import com.stratio.deep.extractor.actions.GetPartitionsAction;
import com.stratio.deep.extractor.actions.HasNextAction;
import com.stratio.deep.extractor.actions.InitIteratorAction;
import com.stratio.deep.extractor.actions.InitSaveAction;
import com.stratio.deep.extractor.actions.SaveAction;
import com.stratio.deep.extractor.client.codecs.ActionDecoder;
import com.stratio.deep.extractor.client.codecs.ResponseEncoder;
import com.stratio.deep.extractor.response.ExtractorInstanceResponse;
import com.stratio.deep.extractor.response.GetPartitionsResponse;
import com.stratio.deep.extractor.response.HasNextElement;
import com.stratio.deep.extractor.response.HasNextResponse;
import com.stratio.deep.extractor.response.InitIteratorResponse;
import com.stratio.deep.extractor.response.InitSaveResponse;
import com.stratio.deep.extractor.response.SaveResponse;

import de.javakaffee.kryoserializers.UUIDSerializer;

public class ExtractorServerInitializer<T> extends ChannelInitializer<SocketChannel> {

  private final SslContext sslCtx;

  public ExtractorServerInitializer(SslContext sslCtx) {
    this.sslCtx = sslCtx;
  }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    if (sslCtx != null) {
      p.addLast(sslCtx.newHandler(ch.alloc()));
    }

      Kryo kryo = new Kryo();
      KryoRegister.registerResponse(kryo);

      Kryo kryo2 = new Kryo();
      KryoRegister.registerAction(kryo2);


    p.addLast(new ActionDecoder(kryo2));
    p.addLast(new ResponseEncoder(kryo, 16 * 1024, -1));

    p.addLast(new ExtractorServerHandler<T>());
  }


}
