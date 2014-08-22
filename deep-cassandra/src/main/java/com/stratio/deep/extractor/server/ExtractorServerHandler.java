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

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.extractor.actions.*;
import com.stratio.deep.extractor.response.*;
import com.stratio.deep.rdd.CassandraExtractor;
import com.stratio.deep.rdd.DeepTokenRange;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ExtractorServerHandler<T> extends SimpleChannelInboundHandler<Action> {

    private CassandraExtractor<T> extractor;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Action action) throws Exception {

        Response response = null;
        switch (action.getType()) {
            case GET_PARTITIONS:
                GetPartitionsAction<T> partitionsAction = (GetPartitionsAction<T>) action;
                response = new GetPartitionsResponse(this.getPartitions(partitionsAction));
                break;
            case CLOSE:
                this.close();
                response = new CloseResponse();
                break;
            case HAS_NEXT:
                HasNextAction<T> hasNextAction = (HasNextAction<T>) action;
                response = new HasNextResponse(this.hastNext(hasNextAction));
                break;
            case NEXT:
                NextAction<T> nextAction = (NextAction<T>) action;
                response = new NextResponse<T>(this.next(nextAction));
                break;
            case INIT_ITERATOR:
                InitIteratorAction<T> initIteratorAction = (InitIteratorAction<T>) action;
                this.initIterator(initIteratorAction);
                response = new InitIteratorResponse();
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


    protected boolean hastNext(HasNextAction hasNextAction) {

        return extractor.hasNext();

    }

    protected T next(NextAction<T> nextAction) {

        return extractor.next();

    }

    protected void close() {
        extractor.close();
        return;

    }

    protected void initIterator(InitIteratorAction<T> initIteratorAction) {
        if (extractor == null) {
            this.initExtractor(initIteratorAction.getConfig());
        }

        extractor.initIterator(initIteratorAction.getPartition(), initIteratorAction.getConfig());
        return;

    }

    protected DeepTokenRange[] getPartitions(GetPartitionsAction<T> getPartitionsAction) {

        if (extractor == null) {
            this.initExtractor(getPartitionsAction.getConfig());
        }

        return extractor.getPartitions(getPartitionsAction.getConfig());
    }


    /**
     * @param config
     */
    @SuppressWarnings("unchecked")
    private void initExtractor(ExtractorConfig<T> config) {

        Class<T> rdd = (Class<T>) config.getRDDClass();
        try {
            Constructor<T> c = null;
            if (config.getEntityClass().isAssignableFrom(Cells.class)){
                c = rdd.getConstructor();
                this.extractor = (CassandraExtractor<T>) c.newInstance();
            }else{
                c = rdd.getConstructor(Class.class);
                this.extractor = (CassandraExtractor<T>) c.newInstance(config.getEntityClass());
            }


        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
