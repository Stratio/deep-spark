package com.stratio.deep.thrift;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by adelapena on 10/09/14.
 */
@Test(testName = "testFetchSortedTokens")
public class ThriftRangeUtilsTest {

    @Test
    public void testFetchSortedTokens() {
        testStringComparableConversion(new Murmur3Partitioner());
        testStringComparableConversion(new RandomPartitioner());
        testStringComparableConversion(new OrderPreservingPartitioner());
        // testStringComparableConversion(new ByteOrderedPartitioner());
    }

    private static void testStringComparableConversion(IPartitioner<?> partitioner) {

        ThriftRangeUtils utils = new ThriftRangeUtils(partitioner, "", 0, "", "", 0);

        Token<?> minToken = partitioner.getMinimumToken();
        Token<?> midPoint = partitioner.midpoint(minToken, minToken);
        Token<?> rndPoint = partitioner.getRandomToken();

        testStringComparableConversion(utils, (Comparable) minToken.token);
        testStringComparableConversion(utils, (Comparable) midPoint.token);
        testStringComparableConversion(utils, (Comparable) rndPoint.token);
    }

    private static void testStringComparableConversion(ThriftRangeUtils utils, Comparable value) {
        String tokenAsString = utils.tokenAsString(value);
        Comparable tokenAsComparable = utils.tokenAsComparable(tokenAsString);
        assertEquals(tokenAsComparable, value);
    }

}
