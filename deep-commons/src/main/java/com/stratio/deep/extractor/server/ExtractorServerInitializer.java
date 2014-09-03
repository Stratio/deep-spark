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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.stratio.deep.extractor.actions.*;
import com.stratio.deep.extractor.client.codecs.ActionDecoder;
import com.stratio.deep.extractor.client.codecs.ResponseEncoder;
import com.stratio.deep.extractor.response.*;
import de.javakaffee.kryoserializers.UUIDSerializer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

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
        registerKryo(kryo);

        Kryo kryo2 = new Kryo();
        registerKryo(kryo2);


        p.addLast(new ActionDecoder(kryo2));
        p.addLast(new ResponseEncoder(kryo,16 * 1024, 64 * 1024));

        p.addLast(new ExtractorServerHandler<T>());
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
