package com.stratio.deep.cassandra.thrift;

import com.stratio.deep.cassandra.thrift.ThriftRangeUtils;
import com.stratio.deep.commons.rdd.DeepTokenRange;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.thrift.CfSplit;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by adelapena on 10/09/14.
 */
@Test(testName = "testFetchSortedTokens")
public class ThriftRangeUtilsTest {

    @Test
    public void testFetchSortedTokensWithMurmur3Partitioner() {
        testStringComparableConversion(new Murmur3Partitioner());
    }

    @Test
    public void testFetchSortedTokensWithRandomPartitioner() {
        testStringComparableConversion(new RandomPartitioner());
    }

    @Test
    public void testFetchSortedTokensWithOrderPreservingPartitioner() {
        testStringComparableConversion(new OrderPreservingPartitioner());
    }

    @Test(enabled = false)
    public void testFetchSortedTokensWithByteOrderedPartitioner() {
        testStringComparableConversion(new ByteOrderedPartitioner());
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

    @Test
    public void testDeepTokenRangesWithMurmur3Partitioner() {
        testDeepTokenRangesAux(new Murmur3Partitioner(), 10L, 20L);
    }

    @Test
    public void testDeepTokenRangesWithRandomPartitioner() {
        testDeepTokenRanges(new RandomPartitioner(), BigInteger.valueOf(10), BigInteger.valueOf(20));
    }

    @Test
    public void testDeepTokenRangesWithOrderPreservingPartitioner() {
        testDeepTokenRanges(new OrderPreservingPartitioner(), "a", "b");
    }

    private static <K extends Comparable, T extends Token<K>> void testDeepTokenRanges(IPartitioner<T> partitioner,
                                                                                          K start,
                                                                                          K end) {
        K min = partitioner.getMinimumToken().token;
        testDeepTokenRangesAux(partitioner, start, end);
        testDeepTokenRangesAux(partitioner, end, start);
        testDeepTokenRangesAux(partitioner, min, end);
        testDeepTokenRangesAux(partitioner, start, min);
        testDeepTokenRangesAux(partitioner, min, min);
    }

    private static <K extends Comparable, T extends Token<K>> void testDeepTokenRangesAux(IPartitioner<T> partitioner,
                                                                                          K start,
                                                                                          K end) {
        int compare = start.compareTo(end);

        List<String> endpoints = Arrays.asList("192.168.0.1", "192.168.0.3", "192.168.0.2");
        if (compare <= 0) {
            List<DeepTokenRange> expectedRanges = Arrays.asList(new DeepTokenRange(start, end, endpoints));
            testDeepTokenRanges(partitioner, start, end, endpoints, expectedRanges);
        } else {
            K min = partitioner.getMinimumToken().token;
            List<DeepTokenRange> expectedRanges = Arrays.asList(
                    new DeepTokenRange(start, min, endpoints), new DeepTokenRange(min, end, endpoints));
            testDeepTokenRanges(partitioner, start, end, endpoints, expectedRanges);
        }
    }

    private static <K extends Comparable, T extends Token<K>> void testDeepTokenRanges(IPartitioner<T> partitioner,
                                                                                       K startToken,
                                                                                       K endToken,
                                                                                       List<String> endpoints,
                                                                                       List<DeepTokenRange> expectedRanges) {

        ThriftRangeUtils utils = new ThriftRangeUtils(partitioner, "", 0, "", "", 0);

        Token.TokenFactory tokenFactory = partitioner.getTokenFactory();
        AbstractType tokenType = partitioner.getTokenValidator();
        String start = tokenFactory.toString(tokenFactory.fromByteArray(tokenType.decompose(startToken)));
        String end = tokenFactory.toString(tokenFactory.fromByteArray(tokenType.decompose(endToken)));
        CfSplit thriftSplit = new CfSplit(start, end, 0);
        List<DeepTokenRange> actualRanges = utils.deepTokenRanges(Arrays.asList(thriftSplit), endpoints);
        assertEquals(actualRanges, expectedRanges);
    }

}
