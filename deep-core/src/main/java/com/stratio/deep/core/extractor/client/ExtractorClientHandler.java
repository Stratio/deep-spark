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



import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.spark.Partition;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.extractor.actions.CloseAction;
import com.stratio.deep.commons.extractor.actions.GetPartitionsAction;
import com.stratio.deep.commons.extractor.actions.HasNextAction;
import com.stratio.deep.commons.extractor.actions.InitIteratorAction;
import com.stratio.deep.commons.extractor.actions.InitSaveAction;
import com.stratio.deep.commons.extractor.actions.NextAction;
import com.stratio.deep.commons.extractor.actions.SaveAction;
import com.stratio.deep.commons.extractor.response.GetPartitionsResponse;
import com.stratio.deep.commons.extractor.response.HasNextResponse;
import com.stratio.deep.commons.extractor.response.NextResponse;
import com.stratio.deep.commons.extractor.response.Response;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;

public class ExtractorClientHandler<T> extends SimpleChannelInboundHandler<Response> implements
        IExtractor<T, ExtractorConfig<T>> {

    private static final long serialVersionUID = -5313035259708180308L;
    // Stateful properties
    private volatile Channel channel;

    private final BlockingQueue<Response> answer = new LinkedBlockingQueue<Response>();

    public ExtractorClientHandler() {
        super(true);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.netty.channel.SimpleChannelInboundHandler#channelRead0(io.netty.channel.ChannelHandlerContext ,
     * java.lang.Object)
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Response msg) throws Exception {
        answer.add(msg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.rdd.IDeepRDD#getPartitions(org.apache.spark.broadcast.Broadcast, int)
     */
    @Override
    public Partition[] getPartitions(ExtractorConfig<T> config) {

        GetPartitionsAction<T> getPartitionsAction = new GetPartitionsAction<>(config);

        channel.writeAndFlush(getPartitionsAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return ((GetPartitionsResponse) response).getPartitions();
    }

    @Override
    public void close() {
        CloseAction closeAction = new CloseAction();

        channel.writeAndFlush(closeAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return;
    }

    @Override
    public boolean hasNext() {
        HasNextAction hasNextAction = new HasNextAction<>();

        channel.writeAndFlush(hasNextAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return ((HasNextResponse) response).getData();
    }

    @Override
    public T next() {
        NextAction<T> nextAction = new NextAction<>();

        channel.writeAndFlush(nextAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return ((NextResponse<T>) response).getData();
    }

    @Override
    public void initIterator(Partition dp, ExtractorConfig<T> config) {
        InitIteratorAction<T> initIteratorAction = new InitIteratorAction<>(dp, config);

        channel.writeAndFlush(initIteratorAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.deep.commons.rdd.IExtractor#saveRDD(java.lang.Object,
     * com.stratio.deep.commons.functions.SaveFunction)
     */
    @Override
    public void saveRDD(T t) {
        SaveAction<T> saveAction = new SaveAction<>(t);

        channel.writeAndFlush(saveAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return;
    }

    @Override
    public void initSave(ExtractorConfig<T> config, T first, UpdateQueryBuilder queryBuilder) {
        InitSaveAction<T> initSaveAction = new InitSaveAction<>(config, first, queryBuilder);

        channel.writeAndFlush(initSaveAction);

        Response response;
        boolean interrupted = false;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return;
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return null;
    }

}
