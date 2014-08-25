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

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.DeepTokenRange;
import com.stratio.deep.rdd.IExtractor;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.spark.Partition;
import org.apache.spark.rdd.RDD;

import javax.net.ssl.SSLException;

/**
 * Sends a list of continent/city pairs to a {@link } to get the local times of the
 * specified cities.
 */
public class ExtractorClient<T> implements IExtractor<T> {

    static final boolean SSL = System.getProperty("ssl") != null;
    //  static final String HOST = System.getProperty("host", "172.19.0.133");
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8463"));

    private transient EventLoopGroup group = new NioEventLoopGroup();

    private transient Channel ch;

    private ExtractorClientHandler<T> handler;

    public void initialize() throws SSLException, InterruptedException {

        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } else {
            sslCtx = null;
        }

        Bootstrap b = new Bootstrap();
        b.group(group).channel(NioSocketChannel.class).handler(new ExtractorClientInitializer<T>(sslCtx));

        // Make a new connection.
        this.ch = b.connect(HOST, PORT).sync().channel();

        // Get the handler instance to initiate the request.
        this.handler = ch.pipeline().get(ExtractorClientHandler.class);
    }

    public void finish() {
        ch.close();
        group.shutdownGracefully();
    }



    @Override
    public boolean hasNext() {
        return handler.hasNext();
    }

    @Override
    public T next() {
        return handler.next();
    }


    @Override
    public void initIterator(Partition dp, ExtractorConfig<T> config) {
        handler.initIterator(dp, config);
    }

    @Override
    public IExtractor getExtractorInstance(ExtractorConfig<T> config) {
        return handler.getExtractorInstance(config);
    }

    @Override
    public void saveRDD(RDD<T> rdd, ExtractorConfig<T> config) {
        throw new DeepGenericException("Not implemented");
    }


    @Override
    public void close() {
        handler.close();
        return ;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.stratio.deep.rdd.IDeepRDD#getPartitions(com.stratio.deep.config.IDeepJobConfig, int)
     */
    @Override
    public Partition[] getPartitions(ExtractorConfig<T> config) {
        return this.handler.getPartitions(config);
    }


}
