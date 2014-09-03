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
        registerKryo(kryo);

        Kryo kryo2 = new Kryo();
        registerKryo(kryo2);


        p.addLast(new ResponseDecoder(kryo));
        p.addLast(new ActionEncoder(kryo2, 4 * 1024, 16 * 1024));

        p.addLast(new ExtractorClientHandler<T>());
    }


    private void registerKryo(Kryo kryo) {
        registerEncoder(kryo);
        registerDecoder(kryo);
        registerUtils(kryo);
    }


    private void registerEncoder(Kryo kryo){
        kryo.register(CloseAction.class);
        kryo.register(ExtractorInstanceAction.class);
        kryo.register(GetPartitionsAction.class);
        kryo.register(HasNextAction.class);
        kryo.register(InitIteratorAction.class);
        kryo.register(InitSaveAction.class);
        kryo.register(NextAction.class);
        kryo.register(SaveAction.class);


    }

    private void registerDecoder(Kryo kryo){
        kryo.register(ExtractorInstanceResponse.class);
        kryo.register(GetPartitionsResponse.class);
        kryo.register(HasNextResponse.class);
        kryo.register(InitIteratorResponse.class);
        kryo.register(InitSaveResponse.class);
        kryo.register(NextResponse.class);
        kryo.register(SaveResponse.class);


    }

    private void registerUtils(Kryo kryo){
        kryo.register(UUID.class, new UUIDSerializer());
    }




    private void registerJavaEncoder(Kryo kryo){
        //kryo.register(Action.class,new JavaSerializer());
        kryo.register(CloseAction.class,new JavaSerializer());
        kryo.register(ExtractorInstanceAction.class,new JavaSerializer());
        kryo.register(GetPartitionsAction.class,new JavaSerializer());
        kryo.register(HasNextAction.class,new JavaSerializer());
        kryo.register(InitIteratorAction.class,new JavaSerializer());
        kryo.register(InitSaveAction.class,new JavaSerializer());
        kryo.register(NextAction.class,new JavaSerializer());
        kryo.register(SaveAction.class,new JavaSerializer());


    }

    private void registerJavaDecoder(Kryo kryo){

        //kryo.register(Response.class,new JavaSerializer());
        kryo.register(ExtractorInstanceResponse.class,new JavaSerializer());
        kryo.register(GetPartitionsResponse.class,new JavaSerializer());
        kryo.register(HasNextResponse.class,new JavaSerializer());
        kryo.register(InitIteratorResponse.class,new JavaSerializer());
        kryo.register(InitSaveResponse.class,new JavaSerializer());
        kryo.register(NextResponse.class,new JavaSerializer());
        kryo.register(SaveResponse.class,new JavaSerializer());

    }

}
