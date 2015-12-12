/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;

import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class BucketManagerTest {

    URL url = BucketManagerTest.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(url.getFile()));


    BucketManager bucketManager;

    @BeforeMethod
    public void setUp() throws Exception {
        bucketManager = new BucketManager(config);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testInitialize_failed_not_continuous() throws Exception {
        Config config = Mockito.mock(Config.class);
        when(config.getShardIds()).thenReturn(new HashSet<>(Arrays.asList("c1", "c2")));
        when(config.getAttributesForShard(any()))
            .thenReturn(createBucketEntry("0-10"))
            .thenReturn(createBucketEntry("12-20"));
        new BucketManager(config);
    }


    @Test(expectedExceptions = IllegalStateException.class)
    public void testInitialize_failed_not_start_from_0() throws Exception {
        Config config = Mockito.mock(Config.class);
        when(config.getShardIds()).thenReturn(new HashSet<>(Collections.singletonList("c1")));
        when(config.getAttributesForShard(any()))
            .thenReturn(createBucketEntry("1-10"));
        new BucketManager(config);
    }

    @Test
    public void testLookupBucketTable() throws Exception {
        assertEquals(bucketManager.lookupBucketTable(0).shardId, "shard1");
        assertEquals(bucketManager.lookupBucketTable(100).shardId, "shard2");
        assertEquals(bucketManager.lookupBucketTable(200).shardId, "shard1");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testLookupBucketTable_failed_out_of_bound() throws Exception {
        bucketManager.lookupBucketTable(-1);
    }

    @Test
    public void testLookupBucketTable_range_success() throws Exception {
        assertEquals(bucketManager.lookupBucketTable(Range.closed(0, 99)).shardId, "shard1");
        assertEquals(bucketManager.lookupBucketTable(Range.closed(0, 50)).shardId, "shard1");
        assertEquals(bucketManager.lookupBucketTable(Range.closed(100, 150)).shardId, "shard2");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testLookupBucketTable_range_failed_overlapped() throws Exception {
        bucketManager.lookupBucketTable(Range.closed(99, 100));
    }


    @Test(expectedExceptions = IllegalStateException.class)
    public void testLookupBucketTable_range_failed_out_of_bound() throws Exception {
        bucketManager.lookupBucketTable(Range.closed(-1, 200));
    }

    @Test
    public void testUpdateBucketRange_success() throws Exception {
        assertEquals(bucketManager.lookupBucketTable(0).shardId, "shard1");
        assertEquals(bucketManager.lookupBucketTable(0).migratingShardId, null);
        assertEquals(bucketManager.lookupBucketTable(100).shardId, "shard2");
        assertEquals(bucketManager.lookupBucketTable(100).migratingShardId, null);
        bucketManager.updateBucketRange(Range.closed(0, 10), "shard1", "shard2", false);
        assertEquals(bucketManager.lookupBucketTable(0).shardId, "shard1");
        assertEquals(bucketManager.lookupBucketTable(0).migratingShardId, "shard2");
        assertEquals(bucketManager.lookupBucketTable(100).shardId, "shard2");
        assertEquals(bucketManager.lookupBucketTable(100).migratingShardId, null);
        bucketManager.updateBucketRange(Range.closed(0, 10), "shard1", "shard2", true);
        assertEquals(bucketManager.lookupBucketTable(0).shardId, "shard2");
        assertEquals(bucketManager.lookupBucketTable(0).migratingShardId, null);
        assertEquals(bucketManager.lookupBucketTable(100).shardId, "shard2");
        assertEquals(bucketManager.lookupBucketTable(100).migratingShardId, null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUpdateBucketRange_failed_overlapped() throws Exception {
        bucketManager.updateBucketRange(Range.closed(99, 100), "shard1", "shard2", false);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUpdateBucketRange_failed_not_in_source_shard() throws Exception {
        bucketManager.updateBucketRange(Range.closed(100, 199), "shard1", "shard2", false);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUpdateBucketRange_failed_out_of_bound() throws Exception {
        bucketManager.updateBucketRange(Range.closed(-1, -1), "shard1", "shard2", false);
    }

    private HashMap<String, String> createBucketEntry(String bucketRange) {
        return new HashMap<String, String>() {{
            put("bucketMap", bucketRange);
        }};
    }

}