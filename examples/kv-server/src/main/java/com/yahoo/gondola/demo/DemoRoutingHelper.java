/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.spi.RoutingHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * The callback that inspect the request and return a shard ID for RoutingFilter.
 */
public class DemoRoutingHelper implements RoutingHelper {

    String hostId;
    Config config;
    int numberOfShards;
    List<String> shardIdList;

    public DemoRoutingHelper(String hostId, Config config) {
        this.hostId = hostId;
        this.config = config;
        loadNumberOfShards();
        loadShardIdList();
    }

    @Override
    public int getBucketId(ContainerRequestContext request) {
        int hashValue = hashUri(request.getUriInfo().getPath());
        return hashValue % numberOfShards;
    }

    @Override
    public String getSiteId(ContainerRequestContext request) {
        return config.getAttributesForHost(hostId).get("siteId");
    }

    private void loadNumberOfShards() {
        Set<String> shardIds = new HashSet<>();
        for (String hostId : config.getHostIds()) {
            shardIds.addAll(config.getShardIds(hostId));
        }
        numberOfShards = shardIds.size();
    }

    private void loadShardIdList() {
        shardIdList = config.getShardIds(hostId);
    }

    /**
     * The URI will be relative to servlet.
     * e.g: entries/key1?foo=123
     */
    private int hashUri(String path) {
        Pattern pattern = Pattern.compile("entries/([^/?]+)");
        Matcher m = pattern.matcher(path);
        return m.find() ? m.group(0).hashCode() : 0;
    }
}
