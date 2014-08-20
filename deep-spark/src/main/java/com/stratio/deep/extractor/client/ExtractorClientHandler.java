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

import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.extractor.actions.*;
import com.stratio.deep.extractor.response.*;
import com.stratio.deep.rdd.IDeepPartition;
import com.stratio.deep.rdd.IDeepRDD;
import com.stratio.deep.utils.Pair;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ExtractorClientHandler<T> extends SimpleChannelInboundHandler<Response> implements
        IDeepRDD<T> {

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
     * @see
     * io.netty.channel.SimpleChannelInboundHandler#channelRead0(io.netty.channel.ChannelHandlerContext
     * , java.lang.Object)
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
    public Partition[] getPartitions(DeepJobConfig<T> config, int id) {

        GetPartitionsAction<T> getPartitionsAction = new GetPartitionsAction<>(config, id);

        channel.writeAndFlush(getPartitionsAction);

        Response response;
        boolean interrupted = false;
        for (; ; ) {
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


    //    @Override
    public java.util.Iterator<T> compute(TaskContext context, IDeepPartition partition, DeepJobConfig<T> config) {
//
////        ComputeAction<T> computeAction = new ComputeAction<T>(context, partition, config);
//
//        channel.writeAndFlush(computeAction);
//
//        Response response;
//        boolean interrupted = false;
//        for (;;) {
//            try {
//                response = answer.take();
//                break;
//            } catch (InterruptedException ignore) {
//                interrupted = true;
//            }
//        }
//
//        if (interrupted) {
//            Thread.currentThread().interrupt();
//        }
//
//        return ((ComputeResponse<T>) response).getData();
        return null;
    }

    @Override
    public boolean hasNext() {
        HasNextAction hasNextAction = new HasNextAction<>();

        channel.writeAndFlush(hasNextAction);

        Response response;
        boolean interrupted = false;
        for (; ; ) {
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
        for (; ; ) {
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
    public void close() {

    }

    @Override
    public void initIterator(IDeepPartition dp, DeepJobConfig<T> config) {
        InitIteratorAction<T> initIteratorAction = new InitIteratorAction<>(dp, config);

        channel.writeAndFlush(initIteratorAction);

        Response response;
        boolean interrupted = false;
        for (; ; ) {
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

    public T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> recordReader,
                              DeepJobConfig<T> config) {

        TransformElementAction<T> transformElementAction = new TransformElementAction<>(recordReader, config);

        channel.writeAndFlush(transformElementAction);

        Response response;
        boolean interrupted = false;
        for (; ; ) {
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

        return ((TransformElementResponse<T>) response).getData();
    }

}
