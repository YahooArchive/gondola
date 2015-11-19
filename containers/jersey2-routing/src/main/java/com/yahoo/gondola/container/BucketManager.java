/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.yahoo.gondola.Config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class BucketManager {

    //bucketId -> shardId.
    private RangeMap<Integer, String> bucketMap = TreeRangeMap.create();
    private Config config;

    public BucketManager(Config config) {
        this.config = config;
        loadBucketTable();
    }


    public String lookupBucketTable(int bucketId) {
        String shardId = bucketMap.get(bucketId);
        if (shardId != null) {
            return shardId;
        }
        throw new IllegalStateException("Bucket ID doesn't exist in bucket table - " + bucketId);
    }

    public String lookupBucketTable(Range<Integer> range) {
        Map<Range<Integer>, String> rangeMaps = bucketMap.subRangeMap(range).asMapOfRanges();
        if (rangeMaps.size() > 1) {
            throw new IllegalStateException();
        } else if (rangeMaps.size() == 0) {
            return null;
        }
        return rangeMaps.values().stream().findFirst().get();
    }

    private void loadBucketTable() {
        Range<Integer> range;
        for (String shardId : config.getShardIds()) {
            Map<String, String> attributesForShard = config.getAttributesForShard(shardId);
            String bucketMapString = attributesForShard.get("bucketMap");
            if (bucketMapString.isEmpty()) {
                continue;
            }
            for (String str : bucketMapString.trim().split(",")) {
                String[] rangePair = str.trim().split("-");
                switch (rangePair.length) {
                    case 1:
                        range = Range.closed(Integer.parseInt(rangePair[0]), Integer.parseInt(rangePair[0]));
                        break;
                    case 2:
                        range = Range.closed(Integer.valueOf(rangePair[0]), Integer.valueOf(rangePair[1]));
                        break;
                    default:
                        throw new IllegalStateException("Range format: x - y or  x, but get " + str);

                }
                if (range.lowerEndpoint() < 0) {
                    throw new IllegalStateException("Bucket id must > 0");
                }
                bucketMap.put(range, shardId);
            }
        }
        bucketContinuousCheck();
    }

    private void bucketContinuousCheck() {
        List<Range<Integer>> sortedRange = bucketMap.asMapOfRanges().keySet().stream()
            .sorted((o1, o2) -> o1.lowerEndpoint() > o2.lowerEndpoint() ? 1 : -1)
            .collect(Collectors.toList());
        boolean status = true;
        Range<Integer> prev = null;
        for (Range<Integer> range : sortedRange) {
            if (prev != null) {
                status = status && (range.lowerEndpoint() - 1) == prev.upperEndpoint();
            } else {
                if (range.lowerEndpoint() != 0) {
                    throw new IllegalStateException("Range must start from 0");
                }
            }
            prev = range;
        }
        if (!status) {
            throw new IllegalStateException("Bucket range must be continuous");
        }
    }

    public void updateBucketRange(Range<Integer> range, String fromShardId, String toShardId) {
        String shardId = lookupBucketTable(range);
        if (!fromShardId.equals(shardId)) {
            throw new IllegalStateException("Range should be a subrange of shardId - " + fromShardId);
        }
        bucketMap.put(range, toShardId);
    }
}
