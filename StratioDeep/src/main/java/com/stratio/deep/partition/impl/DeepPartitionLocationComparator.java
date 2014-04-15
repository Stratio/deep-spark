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

package com.stratio.deep.partition.impl;

import com.stratio.deep.exception.DeepInstantiationException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Comparator;

/**
 * Created by luca on 09/04/14.
 */
public class DeepPartitionLocationComparator implements Comparator<String> {
    private InetAddress hostname;

    public InetAddress getHostname() {
        return hostname;
    }

    public DeepPartitionLocationComparator(){
        try {

            this.hostname = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new DeepInstantiationException(e);
        }
    }

    public DeepPartitionLocationComparator(String hostname) {
        try {
            this.hostname = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            throw new DeepInstantiationException(e);
        }
    }

    @Override
    public int compare(String loc1, String loc2) {
        int result = 0;

        try {
            InetAddress addr1 = InetAddress.getByName(loc1);
            InetAddress addr2 = InetAddress.getByName(loc2);

            if (addr1 == addr2) {
                result = 0;
            } else if (addr1.isLoopbackAddress()){
                result = -1;
            } else if (addr2.isLoopbackAddress()){
                result = 1;
            } else if (addr1.getHostAddress().equals(hostname.getHostAddress())){
                result = -1;
            } else if (addr2.getHostAddress().equals(hostname.getHostAddress())){
                result = 1;
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return result;
    }
}
