/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.demo;

import com.yahoo.gondola.Cluster;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.ClusterIdCallback;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * The callback that inspect the request and return a cluster ID for RoutingFilter.
 */
public class DemoIdCallback implements ClusterIdCallback {

    Gondola gondola;
    int numberOfClusters;
    List<String> clusterIdList;

    public DemoIdCallback(Gondola gondola) {
        this.gondola = gondola;
        loadNumberOfClusters(gondola);
        loadClusterIdList(gondola);

    }

    @Override
    public String getClusterId(ContainerRequestContext request) {
        int hashValue = hashUri(request.getUriInfo().getPath());
        return clusterIdList.get(hashValue % numberOfClusters);
    }

    private void loadNumberOfClusters(Gondola gondola) {
        Config config = gondola.getConfig();

        Set<String> clusterIds = new HashSet<>();
        for (String hostId : config.getHostIds()) {
            clusterIds.addAll(config.getClusterIds(hostId));
        }
        numberOfClusters = clusterIds.size();
    }

    private void loadClusterIdList(Gondola gondola) {
        clusterIdList = gondola.getClustersOnHost().stream()
                          .map(Cluster::getClusterId)
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
