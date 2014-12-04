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
package com.stratio.deep.commons.extractor.client.codecs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.commons.extractor.actions.Action;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class ActionDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(ActionDecoder.class);

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        // Wait until the length prefix is available.
        if (in.readableBytes() < 5) {
            return;
        }

        // Wait until the whole data is available.
        int dataLength = in.readInt();
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }

        // Convert the received data into a new BigInteger.
        byte[] decoded = new byte[dataLength];
        in.readBytes(decoded);

        ByteArrayInputStream bis = new ByteArrayInputStream(decoded);
        ObjectInput inObj = null;
        Action action = null;
        try {
            inObj = new ObjectInputStream(bis);
            action = (Action) inObj.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOG.error(e.getMessage());
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (inObj != null) {
                    inObj.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }

        out.add(action);
    }
}
