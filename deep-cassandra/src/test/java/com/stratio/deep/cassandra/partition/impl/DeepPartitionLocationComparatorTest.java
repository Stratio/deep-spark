/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.cassandra.partition.impl;

import com.stratio.deep.commons.exception.DeepInstantiationException;
import com.stratio.deep.commons.impl.DeepPartitionLocationComparator;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.testng.Assert.fail;

/**
 * Created by luca on 09/04/14.
 */
@Test
public class DeepPartitionLocationComparatorTest {


    public void testComparator() throws UnknownHostException {
        DeepPartitionLocationComparator comparator = null;
        try {
            comparator = new DeepPartitionLocationComparator("not_existent_hostname");

            fail();
        } catch (DeepInstantiationException e) {
            // ok
        }

        comparator = new DeepPartitionLocationComparator("localhost");

        String[] locations = new String[]{"google.com", "localhost", "edition.cnn.com"};

        Arrays.sort(locations, comparator);

        assertArrayEquals(new String[]{"localhost", "google.com", "edition.cnn.com"}, locations);

        comparator = new DeepPartitionLocationComparator("edition.cnn.com");

        Arrays.sort(locations, comparator);

        assertArrayEquals(new String[]{"localhost", "edition.cnn.com", "google.com"}, locations);

        comparator = new DeepPartitionLocationComparator("edition.cnn.com");

        locations = new String[]{"google.com", "edition.cnn.com", "localhost"};
        Arrays.sort(locations, comparator);

        assertArrayEquals(new String[]{"localhost", "edition.cnn.com", "google.com"}, locations);

        comparator = new DeepPartitionLocationComparator();

        String hostname = InetAddress.getLocalHost().getHostName();
        locations = new String[]{"google.com", "edition.cnn.com", hostname};
        Arrays.sort(locations, comparator);

        assertArrayEquals(new String[]{hostname, "google.com", "edition.cnn.com"}, locations);
    }
}
