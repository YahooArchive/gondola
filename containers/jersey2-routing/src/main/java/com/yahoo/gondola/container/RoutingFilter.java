/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.client.SnapshotManagerClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;


/**
 * RoutingFilter is a Jersey2 compatible routing filter that provides routing request to leader host before hitting the
 * resource.
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
    public static final String X_FORWARDED_BY = "X-Forwarded-By";

    /**
     * Routing table Key: memberId, value: HTTP URL.
     */
    RoutingHelper routingHelper;

    /**
     * The Gondola.
     */
    Gondola gondola;

    /**
     * The My shard ids.
     */
    Set<String> myShardIds;

    /**
     * The Routing table. shardId --> list of available servers. (URI)
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

    CommandListener commandListener;
    /**
     * Proxy client help to forward request to remote server.
     */
    ProxyClient proxyClient;

    /**
     * Serialized command executor.
     */
    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();


    /**
     * Lock manager.
     */
    private LockManager lockManager = new LockManager();

    /**
     * Mapping table for serviceUris. hostId --> service URL
     */
    Map<String, String> serviceUris = new HashMap<>();

    /**
     * Flag to enable tracing.
     */
    boolean tracing = false;


    /**
     * The App URI of the server
     */
    String myAppUri;

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
    public RoutingFilter(Gondola gondola, RoutingHelper routingHelper, ProxyClientProvider proxyClientProvider,
                         CommandListenerProvider commandListenerProvider)
        throws ServletException {
        this.gondola = gondola;
        this.routingHelper = routingHelper;
        commandListener = commandListenerProvider.getCommandListner(gondola.getConfig());
        ShardManager shardManager = new ShardManager(this, gondola.getConfig(), null);
        commandListener.setShardManagerHandler(shardManager);
        loadRoutingTable();
        loadBucketTable();
        loadConfig();
        watchGondolaEvent();
        proxyClient = proxyClientProvider.getProxyClient(gondola.getConfig());
    }

    private void loadConfig() {
        gondola.getConfig().registerForUpdates(config -> {
            tracing = config.getBoolean("tracing.router");
        });
    }

    private void watchGondolaEvent() {
        gondola.registerForRoleChanges(roleChangeEvent -> {
            if (roleChangeEvent.leader != null) {
                Config config = gondola.getConfig();
                String appUri = getAppUri(config, config.getMember(roleChangeEvent.leader.getMemberId()).getHostId());
                updateShardRoutingEntries(roleChangeEvent.shard.getShardId(), appUri);
                if (roleChangeEvent.leader.isLocal()) {
                    CompletableFuture.runAsync(() -> {
                        String shardId = roleChangeEvent.shard.getShardId();
                        logger.info("Become leader on shard {}, blocking all requests to the shard....", shardId);
                        lockManager.blockRequestOnShard(shardId);
                        logger.info("Request blocked, wait until raft logs applied to storage...");
                        waitRaftLogSynced(shardId);
                        logger.info("Raft logs are up-to-date, notify application is ready to serve...");
                        routingHelper.beforeServing(shardId);
                        logger.info("Ready for serving, unblocking the requests...");
                        long count = lockManager.unblockRequestOnShard(shardId);
                        logger.info("System is back to serving, unblocked {} requests ...", count);
                    }, singleThreadExecutor).exceptionally(throwable -> {
                        logger.info("Errors while executing leader change event", throwable);
                        return null;
                    });
                }
            }
        });
    }


    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (hasRoutingLoop(requestContext)) {
            requestContext.abortWith(
                Response.status(Response.Status.BAD_REQUEST)
                    .build()
            );
            return;
        }

        int bucketId = routingHelper.getBucketId(requestContext);
        String shardId = getShardId(requestContext);
        if (shardId == null) {
            throw new IllegalStateException("ShardID cannot be null.");
        }

        try {
            lockManager.filterRequest(bucketId, shardId);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        incrementBucketCounter(routingHelper.getBucketId(requestContext));

        if (tracing) {
            List<String> forwardedBy = requestContext.getHeaders().get(X_FORWARDED_BY);
            logger.info("Processing request: {} of shard={}, forwarded={}",
                        requestContext.getUriInfo().getAbsolutePath(), shardId,
                        forwardedBy != null ? forwardedBy.toString() : "");
        }

        if (isMyShard(shardId)) {
            Member leader = getLeader(shardId);

            // Those are the condition that the server should process this request
            // Still under leader election
            if (leader == null) {
                requestContext.abortWith(Response
                                             .status(Response.Status.SERVICE_UNAVAILABLE)
                                             .entity("No leader is available")
                                             .build());
                if (tracing) {
                    logger.info("Leader is not available");
                }
                return;
            } else if (leader.isLocal()) {
                if (tracing) {
                    logger.info("Processing this request");
                }
                return;
            }
        }

        // Proxy the request to other server
        proxyRequestToLeader(requestContext, shardId);
    }

    private boolean hasRoutingLoop(ContainerRequestContext requestContext) {
        String headerString = requestContext.getHeaderString(X_FORWARDED_BY);
        return headerString != null && headerString.contains(myAppUri);
    }

    // Response filter
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
        throws IOException {
        decrementBucketCounter(routingHelper.getBucketId(requestContext));
    }

    /**
     * A compatibility checking tool for routing module.
     */
    public static void configCheck(Config config) {
        StringBuilder sb = new StringBuilder();
        for (String shardId : config.getShardIds()) {
            if (!config.getAttributesForShard(shardId).containsKey("bucketMap")) {
                sb.append("Shard bucketMap attribute is missing on Shard - " + shardId + "\n");
            }
        }

        for (String hostId : config.getHostIds()) {
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
                myAppUri = getAppUri(config, hostId);
                continue;
            }
            String appUri = getAppUri(config, hostId);
            serviceUris.put(hostId, appUri);
            for (String shardId : config.getShardIds(hostId)) {
                List<String> addresses = newRoutingTable.get(shardId);
                if (addresses == null) {
                    addresses = new ArrayList<>();
                    newRoutingTable.put(shardId, addresses);
                }

                addresses.add(appUri);
            }
        }
        routingTable = newRoutingTable;
    }

    private String getAppUri(Config config, String hostId) {
        InetSocketAddress address = config.getAddressForHost(hostId);
        Map<String, String> attrs = config.getAttributesForHost(hostId);
        String
            appUri =
            String.format("%s://%s:%s", attrs.get(APP_SCHEME), address.getHostName(), attrs.get(APP_PORT));
        if (!attrs.containsKey(APP_PORT) || !attrs.containsKey(APP_SCHEME)) {
            throw new IllegalStateException(
                String
                    .format("gondola.hosts[%s] is missing either the %s or %s config values", hostId, APP_PORT,
                            APP_SCHEME));
        }
        return appUri;
    }


    private String getShardId(ContainerRequestContext request) {
        if (routingHelper != null) {
            int bucketId = routingHelper.getBucketId(request);
            if (bucketId == -1 && routingHelper != null) {
                return getAffinityColoShard(request);
            } else {
                return lookupBucketTable(bucketId);
            }
        } else {
            return gondola.getShardsOnHost().get(0).getShardId();
        }
    }


    private boolean isMyShard(String shardId) {
        if (myShardIds == null) {
            myShardIds = gondola.getShardsOnHost().stream()
                .map(Shard::getShardId)
                .collect(Collectors.toSet());
        }
        return myShardIds.contains(shardId);
    }

    private Member getLeader(String shardId) {
        return gondola.getShard(shardId).getLeader();
    }

    private void proxyRequestToLeader(ContainerRequestContext request, String shardId) throws IOException {
        List<String> appUris = lookupRoutingTable(shardId);

        boolean fail = false;
        for (String appUri : appUris) {
            try {
                if (tracing) {
                    String requestPath = ((ContainerRequest) request).getRequestUri().getPath();
                    logger.info("Proxy request to remote server, method={}, URI={}",
                                request.getMethod(), appUri + requestPath);
                }
                List<String> forwardedBy = request.getHeaders().get(X_FORWARDED_BY);
                if (forwardedBy == null) {
                    forwardedBy = new ArrayList<>();
                    request.getHeaders().put(X_FORWARDED_BY, forwardedBy);
                }
                forwardedBy.add(myAppUri);

                Response proxiedResponse = proxyClient.proxyRequest(request, appUri);
                String
                    entity =
                    proxiedResponse.getEntity() != null ? proxiedResponse.getEntity().toString() : "";
                request.abortWith(Response
                                      .status(proxiedResponse.getStatus())
                                      .entity(entity)
                                      .header(X_GONDOLA_LEADER_ADDRESS, appUri)
                                      .build());
                updateRoutingTableIfNeeded(shardId, proxiedResponse);
                if (fail) {
                    updateShardRoutingEntries(shardId, appUri);
                }
                return;
            } catch (IOException e) {
                fail = true;
                logger.error("Error while forwarding request to shard:{} {}", shardId, appUri, e);
            }
        }
        request.abortWith(Response
                              .status(Response.Status.BAD_GATEWAY)
                              .entity("All servers are not available in Shard: " + shardId)
                              .build());
    }

    private void updateRoutingTableIfNeeded(String shardId, Response proxiedResponse) {
        String appUri = proxiedResponse.getHeaderString(X_GONDOLA_LEADER_ADDRESS);
        if (appUri != null) {
            logger.info("New leader found, correct routing table with : shardId={}, appUrl={}", shardId, appUri);
            updateShardRoutingEntries(shardId, appUri);
        }
    }

    /**
     * Moves newAppUrl to the first entry of the routing table.
     */
    private void updateShardRoutingEntries(String shardId, String appUri) {
        List<String> appUris = lookupRoutingTable(shardId);
        List<String> newAppUris = new ArrayList<>(appUris.size());
        newAppUris.add(appUri);
        for (String appUrl : appUris) {
            if (!appUrl.equals(appUri)) {
                newAppUris.add(appUrl);
            }
        }
        logger.info("Update shard '{}' leader as {}", shardId, appUri);
        routingTable.put(shardId, newAppUris);
    }

    /**
     * Finds the leader's URL in the routing table.
     *
     * @param shardId The non-null Gondola shard id
     * @return leader App URL. e.g. http://app1.yahoo.com:4080/
     */
    private List<String> lookupRoutingTable(String shardId) {
        List<String> appUrls = routingTable.get(shardId);
        if (appUrls == null) {
            throw new IllegalStateException("Cannot find routing information for shard ID - " + shardId);
        }
        return appUrls;
    }

    /**
     * bucketId -> shardId bucket size is fixed and shouldn't change, and should be prime number
     */
    List<BucketEntry> bucketTable = new ArrayList<>();

    private class BucketEntry {

        Range<Integer> range;
        String shardId;

        public BucketEntry(Integer a, Integer b, String shardId) {
            if (b == null) {
                this.range = Range.closed(a, a);
            } else {
                this.range = Range.closed(a, b);
            }
            this.shardId = shardId;
        }
    }

    private void loadBucketTable() {
        Config config = gondola.getConfig();
        BucketEntry range;
        for (String shardId : config.getShardIds()) {
            Map<String, String> attributesForShard = config.getAttributesForShard(shardId);
            String bucketMapString = attributesForShard.get("bucketMap");
            for (String str : bucketMapString.trim().split(",")) {
                String[] rangePair = str.trim().split("-");
                switch (rangePair.length) {
                    case 1:
                        range = new BucketEntry(Integer.parseInt(rangePair[0]), null, shardId);
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
                        range = new BucketEntry(min, max, shardId);
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
        BucketEntry prev = null;
        bucketTable.sort((o1, o2) -> o1.range.lowerEndpoint() > o2.range.lowerEndpoint() ? 1 : -1);

        for (BucketEntry r : bucketTable) {
            if (prev == null) {
                if (!r.range.contains(1)) {
                    throw new IllegalStateException("Range must start from 1, Found - " + r.range);
                }
            } else {
                if (r.range.lowerEndpoint() - prev.range.upperEndpoint() != 1) {
                    throw new IllegalStateException(
                        "Range must be continuous, Found - " + prev.range + " - " + r.range);
                }
            }
            prev = r;
        }
    }

    private String lookupBucketTable(int bucketId) {
        for (BucketEntry r : bucketTable) {
            if (r.range.contains(bucketId)) {
                return r.shardId;
            }
        }
        throw new IllegalStateException("Bucket ID doesn't exist in bucket table - " + bucketId);
    }


    private String getAffinityColoShard(ContainerRequestContext request) {
        return getAnyShardInSite(routingHelper.getSiteId(request));
    }

    /**
     * Find random shardId in siteId.
     */
    private String getAnyShardInSite(String siteId) {
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

    private void waitRaftLogSynced(String shardId) {
        boolean synced = false;

        long startTime = System.currentTimeMillis();
        long checkTime = startTime;
        while (!synced) {
            try {
                Thread.sleep(500);
                long now = System.currentTimeMillis();
                int diff = gondola.getShard(shardId).getCommitIndex() - routingHelper.getAppliedIndex(shardId);
                if (now - checkTime > 10000) {
                    checkTime = now;
                    logger.warn("Recovery running for {} seconds, {} logs left", (now - startTime) / 1000, diff);
                }
                synced = diff <= 0;
            } catch (Exception e) {
                logger.info("Unknown error", e);
            }
        }
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();

    void reassignBuckets(Range<Integer> splitRange, String toShard, long timeoutMs)
        throws InterruptedException, TimeoutException, ExecutionException {
        executeTaskWithTimeout(() -> {
            if (toShard.equals("c1")) {
                try {
                    Thread.sleep(timeoutMs * 2);
                } catch (InterruptedException e) {
                    // ignored
                }
            }
            return "";
        }, timeoutMs);
    }

    void waitNoRequestsOnBuckets(Range<Integer> splitRange, long timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        executeTaskWithTimeout(() -> {
            if (splitRange != null) {
                Thread.sleep(timeoutMs * 2);
            }
            return "";
        }, timeoutMs);
    }

    void executeTaskWithTimeout(Callable callable, long timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        executor.submit(callable).get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public LockManager getLockManager() {
        return lockManager;
    }
}
