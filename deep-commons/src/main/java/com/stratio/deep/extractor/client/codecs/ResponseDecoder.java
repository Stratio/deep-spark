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
import com.esotericsoftware.kryo.io.Input;
import com.stratio.deep.extractor.response.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.List;

public class ResponseDecoder extends ByteToMessageDecoder {


    private final Kryo kryo;
    private final Input input = new Input();
    private int length = -1;

    public ResponseDecoder(Kryo kryo) {
        this.kryo = kryo;
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        byte[] decoded = null;


        if (length == -1) {
            // Read length.
            if (in.readableBytes() < 4) return;
            length = in.readInt();
        }

        if (in.readableBytes() < length) return;
        decoded = new byte[length];
        length = -1;

        in.readBytes(decoded);
        input.setBuffer(decoded);

        Object object = kryo.readClassAndObject(input);
        in.readerIndex(input.position()+4);

        out.add((Response) object);

        int dummy = in.readableBytes();
    }
}