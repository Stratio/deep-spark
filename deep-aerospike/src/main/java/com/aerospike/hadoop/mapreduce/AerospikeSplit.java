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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

public class AerospikeSplit
    extends InputSplit
    implements org.apache.hadoop.mapred.InputSplit {

    private String type;
    private String node;
    private String host;
    private int port;
    private String namespace;
    private String setName;
    private String numrangeBin;
    private long numrangeBegin;
    private long numrangeEnd;

    AerospikeSplit() {
    }

    public AerospikeSplit(String type, String node, String host, int port,
                          String ns, String setName, String numrangeBin,
                          long numrangeBegin, long numrangeEnd) {
        this.type = type;
        this.node = node;
        this.host = host;
        this.port = port;
        this.namespace = ns;
        this.setName = setName;
        this.numrangeBin = numrangeBin;
        this.numrangeBegin = numrangeBegin;
        this.numrangeEnd = numrangeEnd;
    }

    public String getType() {
        return type;
    }

    public String getNode() {
        return node;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getNameSpace() {
        return namespace;
    }

    public String getSetName() {
        return setName;
    }

    public String getNumRangeBin() {
        return numrangeBin;
    }

    public long getNumRangeBegin() {
        return numrangeBegin;
    }

    public long getNumRangeEnd() {
        return numrangeEnd;
    }

    public long getLength() {
        return 1;
    }

    public String toString() {
        return type + ':' + node + ":" + host + ":" + port + ":"
            + namespace + ":" + setName;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type);
        Text.writeString(out, node);
        Text.writeString(out, host);
        out.writeInt(port);
        Text.writeString(out, namespace);
        Text.writeString(out, setName);
        Text.writeString(out, numrangeBin);
        out.writeLong(numrangeBegin);
        out.writeLong(numrangeEnd);
    }

    public void readFields(DataInput in) throws IOException {
        type = new String(Text.readString(in));
        node = new String(Text.readString(in));
        host = new String(Text.readString(in));
        port = in.readInt();
        namespace = new String(Text.readString(in));
        setName = new String(Text.readString(in));
        numrangeBin = new String(Text.readString(in));
        numrangeBegin = in.readLong();
        numrangeEnd = in.readLong();
    }

    public String[] getLocations() throws IOException {
        return new String[]{ host };
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
