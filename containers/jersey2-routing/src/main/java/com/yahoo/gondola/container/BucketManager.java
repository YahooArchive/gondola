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
    private RangeMap<Integer, ShardState> bucketMap = TreeRangeMap.create();
    private Config config;
    private int numberOfBuckets;

    public static class ShardState {

        public String shardId;
        public String migratingShardId;

        public ShardState(String shardId, String migratingShardId) {
            this.shardId = shardId;
            this.migratingShardId = migratingShardId;
        }

        @Override
        public String toString() {
            return "ShardState{" + "shardId='" + shardId + '\''
                   + ", migratingShardId='" + migratingShardId + '\''
                   + '}';
        }
    }

    public BucketManager(Config config) {
        this.config = config;
        loadBucketTable();
    }


    public ShardState lookupBucketTable(int bucketId) {
        ShardState shardState = bucketMap.get(bucketId);
        if (shardState == null) {
            throw new IllegalStateException("Bucket ID doesn't exist in bucket table - " + bucketId);
        }
        return shardState;
    }

    public ShardState lookupBucketTable(Range<Integer> range) {
        Map<Range<Integer>, ShardState> rangeMaps = bucketMap.subRangeMap(range).asMapOfRanges();
        if (rangeMaps.size() > 1) {
            throw new IllegalStateException(
                "Overlapped range found - inputRange=" + range + " ranges=" + rangeMaps.toString());
        } else if (rangeMaps.size() == 0) {
            return null;
        }
        return rangeMaps.values().stream().findFirst().get();
    }

    private void loadBucketTable() {
        Range<Integer> range;
        int numBuckets = 0;
        for (String shardId : config.getShardIds()) {
            Map<String, String> attributesForShard = config.getAttributesForShard(shardId);
            String bucketMapString = attributesForShard.get("bucketMap");
            bucketMapString = validateBucketString(shardId, bucketMapString);
            if (bucketMapString == null) {
                continue;
            }

            for (String str : bucketMapString.split(",")) {
                String[] rangePair = str.split("-");
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
                bucketMap.put(range, new ShardState(shardId, null));
            }
        }
        for (Map.Entry<Range<Integer>, ShardState> e : bucketMap.asMapOfRanges().entrySet()) {
            Range<Integer> r = e.getKey();
            numBuckets += (r.upperEndpoint() - r.lowerEndpoint() + 1);
        }
        numberOfBuckets = numBuckets;
        validateBucketMap();
    }

    private String validateBucketString(String shardId, String bucketMapString) {
        if (bucketMapString == null) {
            throw new IllegalStateException("Bucket map must be specified in " + shardId);
        }

        bucketMapString = bucketMapString.replaceAll("\\s", "");
        if (bucketMapString.isEmpty()) {
            return null;
        }

        if (!bucketMapString.matches("^(\\d+-\\d+|\\d+)(,(\\d+-\\d+|\\d+))*")) {
            throw new IllegalStateException("Invalid syntax of bucket map - " + bucketMapString);
        }
        return bucketMapString;
    }

    private void validateBucketMap() {
        List<Range<Integer>> sortedRange = bucketMap.asMapOfRanges().keySet().stream()
            .sorted((o1, o2) -> o1.lowerEndpoint() > o2.lowerEndpoint() ? 1 : -1)
            .collect(Collectors.toList());
        Range<Integer> prev = null;
        for (Range<Integer> range : sortedRange) {
            if (prev != null && (range.lowerEndpoint() - 1) != prev.upperEndpoint()) {
                throw new IllegalStateException(String.format("Range must be contiguous. %s -> %s", prev, range));
            } else if (prev == null && range.lowerEndpoint() != 0) {
                throw new IllegalStateException("Range must start from 0");
            }
            prev = range;
        }
        if (numberOfBuckets == 0) {
            throw new IllegalStateException("Number of bucket must not be 0");
        }
    }

    public void updateBucketRange(Range<Integer> range, String fromShardId, String toShardId,
                                  boolean migrationComplete) {
        ShardState shardState = lookupBucketTable(range);
        if (shardState == null) {
            throw new IllegalStateException("Bucket range not found");
        }

        if (fromShardId == null || toShardId == null) {
            throw new IllegalStateException("fromShardId or toShardId cannot be null");
        }

        if (!migrationComplete) {
            handleMigrationInProgress(range, fromShardId, toShardId, shardState);
        } else {
            handleMigrationComplete(range, fromShardId, toShardId, shardState);
        }
    }

    private void handleMigrationComplete(Range<Integer> range, String fromShardId, String toShardId,
                                         ShardState shardState) {
        if (
            (shardState.shardId.equals(fromShardId) && toShardId.equals(shardState.migratingShardId))
            || (shardState.shardId.equals(fromShardId) && shardState.migratingShardId == null)) {
            ShardState newShardState = new ShardState(toShardId, null);
            bucketMap.put(range, newShardState);
        } else {
            throw new IllegalStateException(String.format(
                "Cannot finish migration if fromShardId=%s-%s & toShardId=%s-%s does not match.", fromShardId,
                shardState.shardId, toShardId, shardState.migratingShardId));
        }
    }

    private void handleMigrationInProgress(Range<Integer> range, String fromShardId, String toShardId,
                                           ShardState shardState) {
        if (!shardState.shardId.equals(fromShardId)) {
            throw new IllegalStateException(
                String.format("Bucket range=%s should be owned by shard=%s, but got shard=%s",
                              range, fromShardId, shardState.shardId));
        }

        if (shardState.migratingShardId != null
            && shardState.migratingShardId.equals(toShardId)
            && !shardState.shardId.equals(fromShardId)) {
            throw new IllegalStateException(
                String.format("Bucket range=%s is migrating to shard=%s, cannot be override by shard=%s",
                              range, shardState.shardId, toShardId));
        }

        ShardState newShardState = new ShardState(shardState.shardId, toShardId);
        bucketMap.put(range, newShardState);
    }

    public int getNumberOfBuckets() {
        return numberOfBuckets;
    }

    public RangeMap<Integer, ShardState> getBucketMap() {
        return bucketMap;
    }

    public void rollbackBuckets(Range<Integer> range) {
        ShardState shardState = lookupBucketTable(range);
        shardState.migratingShardId = null;
    }
}
