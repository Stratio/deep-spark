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
package com.stratio.deep.extractor.client.codecs;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.stratio.deep.extractor.actions.Action;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class ActionEncoder extends MessageToByteEncoder<Action> {


    protected void encode(ChannelHandlerContext ctx, Action action, ByteBuf out) {



        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput outObj = null;

        try {
            outObj = new ObjectOutputStream(bos);
            outObj.writeObject(action);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (outObj != null) {
                    outObj.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }

        byte[] encodedObj = bos.toByteArray();
        int dataLength = encodedObj.length;
        out.writeInt(dataLength);
        out.writeBytes(encodedObj);
    }
}
