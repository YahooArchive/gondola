/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Cluster;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.client.SnapshotManagerClient;
import com.yahoo.gondola.container.client.StatClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;


/**
 * RoutingFilter is a Jersey2 compatible routing filter that provides routing request to leader host before
 * hitting the resource.
 */
public class RoutingFilter implements ContainerRequestFilter, ContainerResponseFilter {
    static Logger logger = LoggerFactory.getLogger(RoutingFilter.class);

    /**
     * The constant X_GONDOLA_LEADER_ADDRESS.
     */
    public static final String X_GONDOLA_LEADER_ADDRESS = "X-Gondola-Leader-Address";
    /**
     * The constant APP_PORT.
     */
    public static final String APP_PORT = "appPort";
    /**
     * The constant APP_SCHEME.
     */
    public static final String APP_SCHEME = "appScheme";
    public static final int RETRY = 3;

    /**
     * Routing table Key: memberId, value: HTTP URL.
     */
    RoutingHelper routingHelper;

    /**
     * The Gondola.
     */
    Gondola gondola;

    /**
     * The My cluster ids.
     */
    Set<String> myClusterIds;

    /**
     * The Routing table. clusterId --> list of available servers
     */
    Map<String, List<String>> routingTable;

    /**
     * The Snapshot manager.
     */
    SnapshotManagerClient snapshotManagerClient;

    /**
     * The Bucket request counters. bucketId --> requestCounter
     */
    Map<Integer, AtomicInteger> bucketRequestCounters = new ConcurrentHashMap<>();

    /**
     * The Command adaptor.
     */
    CommandAdaptor commandAdaptor;

    /**
     * Proxy client help to forward request to remote server.
     */
    ProxyClient proxyClient;

    /**
     * Disallow default constructor
     */
    private RoutingFilter() {
    }

    /**
     * Instantiates a new Routing filter.
     *
     * @param gondola       the gondola
     * @param routingHelper the routing helper
     * @throws ServletException the servlet exception
     */
    public RoutingFilter(Gondola gondola, RoutingHelper routingHelper, ProxyClientProvider proxyClientProvider)
        throws ServletException {
        this.gondola = gondola;
        this.routingHelper = routingHelper;
        commandAdaptor = new CommandAdaptor(new CommandHandler());
        loadRoutingTable();
        loadBucketTable();
        watchGondolaEvent();
        proxyClient = proxyClientProvider.getProxyClient(gondola.getConfig());
    }

    private void watchGondolaEvent() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        gondola.registerForRoleChanges(roleChangeEvent -> {
            if (roleChangeEvent.leader != null && roleChangeEvent.leader.isLocal()) {
                singleThreadExecutor.submit(() -> {
                    String clusterId = roleChangeEvent.cluster.getClusterId();
                    blockRequestOnCluster(clusterId);
                    routingHelper.clearState(clusterId);
                    waitSynced(clusterId);
                    unblockRequestOnCluster(clusterId);
                });
            }
        });
    }

    /**
     * The Blocked buckets.
     */
    Set<Integer> blockedBuckets;

    // Request Filter
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        int bucketId = routingHelper.getBucketId(requestContext);
        blockRequestIfNeeded(bucketId);
        incrementBucketCounter(routingHelper.getBucketId(requestContext));
        String clusterId = getClusterId(requestContext);

        if (isMyCluster(clusterId)) {
            Member leader = getLeader(clusterId);

            // Those are the condition that the server should process this request
            // Still under leader election
            if (leader == null) {
                requestContext.abortWith(Response
                                             .status(Response.Status.SERVICE_UNAVAILABLE)
                                             .entity("No leader is available")
                                             .build());
                return;
            } else if (leader.isLocal()) {
                return;
            }
        }

        // Proxy the request to other server
        proxyRequestToLeader(requestContext, clusterId);
    }

    private void blockRequestIfNeeded(int bucketId) {
        // TODO
    }

    // Response filter
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
        throws IOException {
        decrementBucketCounter(routingHelper.getBucketId(requestContext));
    }

    /**
     * A compatibility checking tool for routing module.
     *
     * @param config
     */
    public static void configCheck(Config config) {
        StringBuilder sb = new StringBuilder();
        for (String clusterId : config.getClusterIds()) {
            if (!config.getAttributesForCluster(clusterId).containsKey("bucketMap")) {
                sb.append("Cluster bucketMap attribute is missing on Cluster - " + clusterId + "\n");
            }
        }

        for (String hostId :config.getHostIds()) {
            Map<String, String> attributes = config.getAttributesForHost(hostId);
            if (!attributes.containsKey("appScheme") || !attributes.containsKey("appPort")) {
                sb.append("Host attributes appScheme and appPort is missing on Host - " + hostId + "\n");
            }
        }

        if (!sb.toString().isEmpty()) {
            throw new IllegalStateException("Configuration Error: " + sb.toString());
        }
    }

    private void loadRoutingTable() {
        // The routing entry will be modified on the fly, concurrent map is needed
        Map<String, List<String>> newRoutingTable = new ConcurrentHashMap<>();
        Config config = gondola.getConfig();
        for (String hostId : config.getHostIds()) {
            if (hostId.equals(gondola.getHostId())) {
                continue;
            }
            for (String clusterId : config.getClusterIds(hostId)) {
                InetSocketAddress address = config.getAddressForHost(hostId);
                List<String> addresses = newRoutingTable.get(clusterId);
                if (addresses == null) {
                    addresses = new ArrayList<>();
                    newRoutingTable.put(clusterId, addresses);
                }
                Map<String, String> attrs = config.getAttributesForHost(hostId);
                if (attrs.get(APP_PORT) == null || attrs.get(APP_SCHEME) == null) {
                    throw new IllegalStateException(
                        String
                            .format("gondola.hosts[%s] is missing either the %s or %s config values", hostId, APP_PORT,
                                    APP_SCHEME));
                }
                String
                    appUri =
                    String.format("%s://%s:%s", attrs.get(APP_SCHEME), address.getHostName(), attrs.get(APP_PORT));
                addresses.add(appUri);
            }
        }
        routingTable = newRoutingTable;
    }


    private String getClusterId(ContainerRequestContext request) {
        if (routingHelper != null) {
            int bucketId = routingHelper.getBucketId(request);
            if (bucketId == -1 && routingHelper != null) {
                return getAffinityColoCluster(request);
            } else {
                return lookupBucketTable(bucketId);
            }
        } else {
            return gondola.getClustersOnHost().get(0).getClusterId();
        }
    }


    private boolean isMyCluster(String clusterId) {
        if (myClusterIds == null) {
            myClusterIds = gondola.getClustersOnHost().stream()
                .map(Cluster::getClusterId)
                .collect(Collectors.toSet());
        }
        return myClusterIds.contains(clusterId);
    }

    private Member getLeader(String clusterId) {
        return gondola.getCluster(clusterId).getLeader();
    }

    private void proxyRequestToLeader(ContainerRequestContext request, String clusterId) throws IOException {
        List<String> appUrls = lookupRoutingTable(clusterId);

        for (String appUrl : appUrls) {
            try {
                Response proxiedResponse = proxyClient.proxyRequest(request, appUrl);
                String
                    entity =
                    proxiedResponse.getEntity() != null ? proxiedResponse.getEntity().toString() : "";
                request.abortWith(Response
                                      .status(proxiedResponse.getStatus())
                                      .entity(entity)
                                      .header(X_GONDOLA_LEADER_ADDRESS, appUrl)
                                      .build());
                updateRoutingTableIfNeeded(clusterId, proxiedResponse);
                return;
            } catch (IOException e) {
                // TODO: add a config to show stack trace
                logger.error(String.format("Error while forwarding request to %s: %s", appUrl, e.getMessage()));
            }
        }
        request.abortWith(Response
                              .status(Response.Status.BAD_GATEWAY)
                              .entity("All servers are not available in Cluster: " + clusterId)
                              .build());
    }

    private void updateRoutingTableIfNeeded(String clusterId, Response proxiedResponse) {
        String headerString = proxiedResponse.getHeaderString(X_GONDOLA_LEADER_ADDRESS);
        setClusterLeader(clusterId, headerString);
    }

    /**
     * Moves newAppUrl to the first entry of the routing table.
     */
    private void setClusterLeader(String clusterId, String newAppUrl) {
        List<String> appUrls = lookupRoutingTable(clusterId);
        List<String> newAppUrls = new ArrayList<>(appUrls.size());
        newAppUrls.add(newAppUrl);
        for (String appUrl : appUrls) {
            if (!appUrl.equals(newAppUrl)) {
                newAppUrls.add(appUrl);
            }
        }
        routingTable.put(clusterId, newAppUrls);
    }

    /**
     * Finds the leader's URL in the routing table.
     *
     * @param clusterId The non-null Gondola cluster id
     * @return leader App URL. e.g. http://app1.yahoo.com:4080/
     */
    private List<String> lookupRoutingTable(String clusterId) {
        List<String> appUrls = routingTable.get(clusterId);
        if (appUrls == null) {
            throw new IllegalStateException("Cannot find routing information for cluster ID - " + clusterId);
        }
        return appUrls;
    }

    /**
     * bucketId -> clusterId bucket size is fixed and shouldn't change, and should be prime number
     */
    List<Range> bucketTable = new ArrayList<>();

    private class Range {
        Integer minimum;
        Integer maximum;
        String clusterId;

        public Range(Integer minimum, Integer maximum, String clusterId) {
            this.minimum = minimum;
            this.maximum = maximum;
            this.clusterId = clusterId;
        }

        public boolean inRange(int i) {
            return i >= minimum && i <= maximum;
        }

        @Override
        public String toString() {
            return "[" + minimum + ", " + maximum + "]";
        }
    }

    private void loadBucketTable() {
        Config config = gondola.getConfig();
        Range range;
        for (String clusterId : config.getClusterIds()) {
            Map<String, String> attributesForCluster = config.getAttributesForCluster(clusterId);
            String bucketMapString = attributesForCluster.get("bucketMap");
            if (bucketMapString == null) {
                throw new IllegalStateException(
                    "The cluster definition in the config file is missing the 'bucketMap' attribute");
            }

            for (String str : bucketMapString.trim().split(",")) {
                String[] rangePair = str.trim().split("-");
                switch (rangePair.length) {
                    case 1:
                        range = new Range(Integer.parseInt(rangePair[0]), null, clusterId);
                        break;
                    case 2:
                        Integer min, max;
                        min = Integer.valueOf(rangePair[0]);
                        max = Integer.valueOf(rangePair[1]);
                        if (min > max) {
                            Integer tmp = max;
                            max = min;
                            min = tmp;
                        }
                        if (min.equals(max)) {
                            max = null;
                        }
                        range = new Range(min, max, clusterId);
                        break;
                    default:
                        throw new IllegalStateException("Range format: x - y or  x, but get " + str);

                }
                bucketTable.add(range);
            }
        }
        bucketMapCheck();
    }

    private void bucketMapCheck() {
        bucketTable.sort((o1, o2) -> o1.minimum > o2.minimum ? 1 : -1);
        Range prev = null;
        for (Range r : bucketTable) {
            if (prev == null) {
                if (r.minimum != 0) {
                    throw new IllegalStateException("Range must start from 0, Found - " + r.minimum);
                }
            } else {
                if (r.minimum - prev.maximum != 1) {
                    throw new IllegalStateException("Range must continuous, Found - " + prev + " - " + r);
                }
            }
            prev = r;
        }
    }

    private String lookupBucketTable(int bucketId) {
        for (Range r : bucketTable) {
            if (r.inRange(bucketId)) {
                return r.clusterId;
            }
        }
        throw new IllegalStateException("Bucket ID doesn't exist in bucket table - " + bucketId);
    }

    /**
     * Returns the migration type by inspect config, DB -> if two clusters use different database APP ->
     * if two clusters use same database.
     */
    private MigrationType getMigrationType(String fromCluster, String toCluster) {
        //TODO: implement
        return MigrationType.APP;
    }

    /**
     * Helper function to get clusterId of the bucketId.
     */
    private String getClusterIdByBucketId(int bucketId) {
        //TODO: implement
        return null;
    }

    /**
     * Two types of migration APP -> Shared the same DB DB  -> DB migration
     */
    enum MigrationType {
        APP,
        DB
    }

    private String getAffinityColoCluster(ContainerRequestContext request) {
        return getAnyClusterInSite(routingHelper.getSiteId(request));
    }

    /**
     * Find random clusterId in siteId.
     */
    private String getAnyClusterInSite(String siteId) {
        //TODO: implement
        return null;
    }

    private int incrementBucketCounter(int bucketId) {
        AtomicInteger counter = getCounter(bucketId);
        return counter.incrementAndGet();
    }

    private int decrementBucketCounter(int bucketId) {
        AtomicInteger counter = getCounter(bucketId);
        return counter.decrementAndGet();
    }

    private AtomicInteger getCounter(int bucketId) {
        AtomicInteger counter = bucketRequestCounters.get(bucketId);
        if (counter == null) {
            AtomicInteger newCounter = new AtomicInteger();
            AtomicInteger existingCounter = bucketRequestCounters.putIfAbsent(bucketId, newCounter);
            counter = existingCounter == null ? newCounter : existingCounter;
        }
        return counter;
    }

    private void waitSynced(String clusterId) {
        boolean synced = false;

        long startTime = System.currentTimeMillis();
        long checkTime = startTime;
        while (!synced) {
            try {
                Thread.sleep(500);
                long now = System.currentTimeMillis();
                int diff = gondola.getCluster(clusterId).getLastSavedIndex() - routingHelper.getAppliedIndex(clusterId);
                if (checkTime - now > 10000) {
                    checkTime = now;
                    logger.warn("Recovery running for {} seconds, {} logs left", now - startTime, diff);
                }
                synced = diff == 0;
            } catch (Exception e) {
                logger.info("Unknown error", e);
            }
        }
    }

    private void unblockRequestOnCluster(String clusterId) {
        logger.info("unblock request on cluster : {}", clusterId);
        // TODO
    }

    private void blockRequestOnCluster(String clusterId) {
        logger.info("block requests on cluster : {}", clusterId);
        // TODO
    }

    private void unblockRequest() {
        logger.info("unblock all requests");
        // TODO:
    }

    private void blockRequest(long timeoutMs) {
        logger.info("block all requests");
        // TODO:
    }

    private void unblockRequestOnBuckets(String splitRange) {
        logger.info("unblock requests on buckets : {}", splitRange);
        // TODO:
    }

    private void blockRequestOnBuckets(String splitRange, long timeoutMs) {
        logger.info("block requests on buckets : {}", splitRange);
        // TODO:
    }

    private void reassignBuckets(String splitRange, String toCluster) {
        // TODO:
    }

    private void waitNoRequestsOnBuckets(String splitRange, long timeoutMs) {
        // TODO:
    }

    private String getSplitRange(String fromCluster) {
        // TODO:
        return null;
    }

    /**
     * The type Command handler.
     */
    class CommandHandler implements ShardManager {

        /**
         * The Stat client.
         */
        StatClient statClient;

        @Override
        public void allowObserver() {
            // TODO: gondola allow observer
        }

        @Override
        public void disallowObserver() {
            // TODO: gondola allow observer
        }

        @Override
        public void startObserving(String clusterId) {
            // TODO: gondola start observing
        }

        @Override
        public void stopObserving(String clusterId) {
            // TODO: gondola stop observing
        }

        @Override
        public void splitBucket(String fromCluster, String toCluster, long timeoutMs) {
            String splitRange = getSplitRange(fromCluster);
            MigrationType migrationType = getMigrationType(fromCluster, toCluster);
            switch (migrationType) {
                case APP:
                    for (int i = 0; i < RETRY; i++) {
                        try {
                            blockRequestOnBuckets(splitRange, 5000L);
                            waitNoRequestsOnBuckets(splitRange, 5000L);
                            reassignBuckets(splitRange, toCluster);
                            break;
                        } finally {
                            unblockRequestOnBuckets(splitRange);
                        }
                    }
                    break;
                case DB:
                    for (int i = 0; i < RETRY; i++) {
                        try {
                            statClient.waitApproaching(toCluster, -1L);
                            blockRequest(5000L);
                            statClient.waitSynced(toCluster, 5000L);
                            reassignBuckets(splitRange, toCluster);
                        } finally {
                            unblockRequest();
                        }
                        break;
                    }
            }
        }

        @Override
        public void mergeBucket(String fromCluster, String toCluster, long timeoutMs) {

        }
    }
}
