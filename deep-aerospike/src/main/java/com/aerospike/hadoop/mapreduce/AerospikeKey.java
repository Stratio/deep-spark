/* 
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.aerospike.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Unpacker.ObjectUnpacker;

public class AerospikeKey   implements WritableComparable {

    public String namespace;
    public String setName;
    public byte[] digest;
    public Value userKey;

    public AerospikeKey() {
        this.namespace = null;
        this.setName = null;
        this.digest = null;
        this.userKey = null;
    }

    public AerospikeKey(Key key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public AerospikeKey(AerospikeKey key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public void set(Key key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public void set(AerospikeKey key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public Key toKey() {
        return new Key(namespace, digest, setName, userKey);
    }

    public void write(DataOutput out) throws IOException {
        try {
            out.writeUTF(namespace);
            out.writeUTF(setName);
            out.writeInt(digest.length);
            out.write(digest);
            out.writeBoolean(userKey != null);
            if (userKey == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Packer pack = new Packer();
                pack.packObject(userKey);
                byte[] buff = pack.toByteArray();
                out.writeInt(buff.length);
                out.write(buff);
            }
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void readFields(DataInput in) throws IOException {
        try {
            namespace = in.readUTF();
            setName = in.readUTF();
            int digestLen = in.readInt();
            digest = new byte[digestLen];
            in.readFully(digest);
            if (in.readBoolean()) {
                int buflen = in.readInt();
                byte[] buff = new byte[buflen];
                in.readFully(buff);
                ObjectUnpacker unpack = new ObjectUnpacker(buff, 0, buff.length);
                userKey = Value.get(unpack.unpackObject());
            }
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }
       
    public static AerospikeKey read(DataInput in) throws IOException {
        AerospikeKey key = new AerospikeKey();
        key.readFields(in);
        return key;
    }

    public int compareTo(Object obj) {
        AerospikeKey other = (AerospikeKey) obj;
        byte[] left = this.digest;
        byte[] right = other.digest;
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
