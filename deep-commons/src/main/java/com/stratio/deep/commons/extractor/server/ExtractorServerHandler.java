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
package com.stratio.deep.commons.extractor.server;

import com.stratio.deep.commons.exception.DeepInstantiationException;
import com.stratio.deep.commons.extractor.actions.*;
import com.stratio.deep.commons.extractor.response.*;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.rdd.IExtractor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ExtractorServerHandler<T> extends SimpleChannelInboundHandler<Action> {

    private static final Logger LOG = Logger.getLogger(ExtractorServerHandler.class);

    private IExtractor<T> extractor;

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
            case SAVE:
                SaveAction<T> SaveAction = (SaveAction<T>) action;
                this.save(SaveAction);
                response = new SaveResponse();
                break;
            case INIT_SAVE:
                InitSaveAction<T> initSave = (InitSaveAction<T>) action;
                this.initSave(initSave);
                response = new InitSaveResponse();
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

    protected Partition[] getPartitions(GetPartitionsAction<T> getPartitionsAction) {

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

        Class<T> rdd = (Class<T>) config.getExtractorImplClass();
        try {
            Constructor<T> c = null;
            if (config.getEntityClass().isAssignableFrom(Cells.class)){
                c = rdd.getConstructor();
            this.extractor = (IExtractor<T>) c.newInstance();
            }else{
                c = rdd.getConstructor(Class.class);
                this.extractor = (IExtractor<T>) c.newInstance(config.getEntityClass());
            }


        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            LOG.error("Impossible to make an extractor instance, check classpath " +e.getMessage());
            throw new DeepInstantiationException("Impossible to make an extractor instance, check classpath " +e.getMessage());
        }
    }

    protected void initSave(InitSaveAction<T> initSaveAction) {
        if (extractor == null) {
            this.initExtractor(initSaveAction.getConfig());
        }

        extractor.initSave(initSaveAction.getConfig(), initSaveAction.getFirst());
        return;

    }

    protected void save(SaveAction<T> saveAction) {
        extractor.saveRDD(saveAction.getRecord());
        return;

    }
}
