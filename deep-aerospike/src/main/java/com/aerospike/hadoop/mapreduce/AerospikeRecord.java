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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Unpacker.ObjectUnpacker;

public class AerospikeRecord implements Writable {

    public Map<String,Object> bins;
    public int generation;
    public int expiration;

    public AerospikeRecord() {
        this.bins = null;
        this.generation = 0;
        this.expiration = 0;
    }

    public AerospikeRecord(Record rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public AerospikeRecord(AerospikeRecord rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public void set(Record rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public void set(AerospikeRecord rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public Record toRecord() {
        return new Record(bins, generation, expiration);
    }

    public void write(DataOutput out) throws IOException {
        try {
            out.writeInt(generation);
            out.writeInt(expiration);
            out.writeInt(bins.size());
            for (Map.Entry<String, Object> entry : bins.entrySet()) {
                out.writeUTF(entry.getKey());
                Packer pack = new Packer();
                pack.packObject(entry.getValue());
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
            generation = in.readInt();
            expiration = in.readInt();
            int nbins = in.readInt();
            bins = new HashMap<String, Object>();
            for (int ii = 0; ii < nbins; ++ii) {
                String key = in.readUTF();
                int buflen = in.readInt();
                byte[] buff = new byte[buflen];
                in.readFully(buff);
                ObjectUnpacker unpack = new ObjectUnpacker(buff, 0, buff.length);
                Object obj = unpack.unpackObject();
                bins.put(key, obj);
            }
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }
       
    public static AerospikeRecord read(DataInput in) throws IOException {
        AerospikeRecord rec = new AerospikeRecord();
        rec.readFields(in);
        return rec;
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
