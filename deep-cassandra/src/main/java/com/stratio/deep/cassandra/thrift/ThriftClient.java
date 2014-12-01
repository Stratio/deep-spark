/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.cassandra.thrift;

import java.io.Closeable;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * {@link org.apache.cassandra.thrift.Cassandra.Client} that encapsulates the transport management.
 * Its {@link #close()} method closes the underlying transport.
 *
 * @author Andres de la Pena <andres@stratio.com>
 */
public class ThriftClient extends Cassandra.Client implements Closeable {

    /**
     * {@inheritDoc}
     */
    private ThriftClient(TProtocol protocol) {
        super(protocol);
    }

    /**
     * Returns a new client for the specified host, setting the specified keyspace.
     *
     * @param host     the Cassandra host address.
     * @param port     the Cassandra host RPC port.
     * @param keyspace the name of the Cassandra keyspace to set.
     * @return a new client for the specified host, setting the specified keyspace.
     * @throws TException if there is any problem with the {@code set_keyspace} call.
     */
    public static ThriftClient build(String host, int port, String keyspace) throws TException {
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        TProtocol protocol = new TBinaryProtocol(transport);
        ThriftClient client = new ThriftClient(protocol);
        transport.open();
        if (keyspace != null) {
            client.set_keyspace(keyspace);
        }
        return client;
    }

    /**
     * Returns a new client for the specified host.
     *
     * @param host the Cassandra host address.
     * @param port the Cassandra host RPC port.
     * @return a new client for the specified host.
     * @throws TException if there is any problem with the {@code set_keyspace} call.
     */
    public static ThriftClient build(String host, int port) throws TException {
        return build(host, port, null);
    }

    /**
     * Closes this client's transport.
     */
    @Override
    public void close() {
        getInputProtocol().getTransport().close();
    }
}
