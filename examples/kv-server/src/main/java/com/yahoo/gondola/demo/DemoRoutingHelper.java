/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Shard;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.spi.RoutingHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * The callback that inspect the request and return a shard ID for RoutingFilter.
 */
public class DemoRoutingHelper implements RoutingHelper {

    Gondola gondola;
    int numberOfShards;
    List<String> shardIdList;
    DemoService demoService;

    public DemoRoutingHelper(Gondola gondola, DemoService demoService) {
        this.gondola = gondola;
        this.demoService = demoService;
        loadNumberOfShards(gondola);
        loadShardIdList(gondola);
    }

    @Override
    public int getBucketId(ContainerRequestContext request) {
        int hashValue = hashUri(request.getUriInfo().getPath());
        return hashValue % numberOfShards;
    }

    @Override
    public int getAppliedIndex(String shardId) {
        return demoService.getAppliedIndex();
    }

    @Override
    public String getSiteId(ContainerRequestContext request) {
        return gondola.getConfig().getAttributesForHost(gondola.getHostId()).get("siteId");
    }

    @Override
    public void beforeServing(String shardId) {
        demoService.beforeServing();
    }

    private void loadNumberOfShards(Gondola gondola) {
        Config config = gondola.getConfig();

        Set<String> shardIds = new HashSet<>();
        for (String hostId : config.getHostIds()) {
            shardIds.addAll(config.getShardIds(hostId));
        }
        numberOfShards = shardIds.size();
    }

    private void loadShardIdList(Gondola gondola) {
        shardIdList = gondola.getShardsOnHost().stream()
                          .map(Shard::getShardId)
                          .collect(Collectors.toList());
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
