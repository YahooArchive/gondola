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
import java.util.concurrent.CompletableFuture;
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
 * RoutingFilter is a Jersey2 compatible routing filter that provides routing request to leader host before hitting the
 * resource.
 */
public class RoutingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static Logger logger = LoggerFactory.getLogger(RoutingFilter.class);

    static final String X_GONDOLA_LEADER_ADDRESS = "X-Gondola-Leader-Address";
    static final String X_FORWARDED_BY = "X-Forwarded-By";
    static final String APP_PORT = "appPort";
    static final String APP_SCHEME = "appScheme";

    private RoutingHelper routingHelper;
    private Gondola gondola;
    private Set<String> myShardIds;

    // shardId --> list of available servers. (URI)
    Map<String, List<String>> routingTable;

    private SnapshotManagerClient snapshotManagerClient;
    private Map<Integer, AtomicInteger> bucketRequestCounters = new ConcurrentHashMap<>();
    private CommandListener commandListener;
    private ProxyClient proxyClient;

    // Serialized command executor.
    private ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    private LockManager lockManager;

    private Map<String, String> serviceUris = new HashMap<>();

    private boolean tracing = false;
    private String myAppUri;
    BucketManager bucketManager;

    /**
     * Disallow default constructor.
     */
    private RoutingFilter() {
    }

    /**
     * Instantiates a new Routing filter.
     *
     * @param gondola                 the gondola
     * @param routingHelper           the routing helper
     * @param proxyClientProvider     the proxy client provider
     * @param commandListenerProvider the command listener provider
     * @throws ServletException the servlet exception
     */
    public RoutingFilter(Gondola gondola, RoutingHelper routingHelper, ProxyClientProvider proxyClientProvider,
                         CommandListenerProvider commandListenerProvider)
        throws ServletException {
        this.gondola = gondola;
        this.routingHelper = routingHelper;
        lockManager = new LockManager(gondola.getConfig());
        commandListener = commandListenerProvider.getCommandListner(gondola.getConfig());
        ShardManager shardManager = new ShardManager(this, gondola.getConfig(), null);
        commandListener.setShardManagerHandler(shardManager);
        bucketManager = new BucketManager(gondola.getConfig());
        loadRoutingTable();
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
                        tracing("Become leader on shard {}, blocking all requests to the shard....", shardId);
                        lockManager.blockRequestOnShard(shardId);
                        tracing("Wait until raft logs applied to storage...");
                        waitRaftLogSynced(shardId);
                        tracing("Raft logs are up-to-date, notify application is ready to serve...");
                        routingHelper.beforeServing(shardId);
                        tracing("Ready for serving, unblocking the requests...");
                        long count = lockManager.unblockRequestOnShard(shardId);
                        tracing("System is back to serving, unblocked {} requests ...", count);
                    }, singleThreadExecutor).exceptionally(throwable -> {
                        logger.info("Errors while executing leader change event", throwable);
                        return null;
                    });
                }
            }
        });
    }

    void tracing(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }


    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (processRoutingLoop(requestContext)) {
            return;
        }

        processRequest(requestContext);
    }

    private void processRequest(ContainerRequestContext requestContext) throws IOException {
        int bucketId = routingHelper.getBucketId(requestContext);
        String shardId = getShardId(requestContext);
        if (shardId == null) {
            requestContext.abortWith(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity("Cannot find shard for bucketId=" + bucketId)
                    .build());
            return;
        }

        try {
            lockManager.filterRequest(bucketId, shardId);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        incrementBucketCounter(routingHelper.getBucketId(requestContext));

        tracing("Processing request: {} of shard={}, forwarded={}",
                requestContext.getUriInfo().getAbsolutePath(), shardId,
                requestContext.getHeaders().containsKey(X_FORWARDED_BY) ? requestContext.getHeaders()
                    .get(X_FORWARDED_BY).toString() : "");

        // redirect the request to other shard
        if (!isMyShard(shardId)) {
            proxyRequestToLeader(requestContext, shardId);
            return;
        }

        Member leader = getLeader(shardId);
        // Those are the condition that the server should process this request
        // Still under leader election
        if (leader == null) {
            requestContext.abortWith(Response
                                         .status(Response.Status.SERVICE_UNAVAILABLE)
                                         .entity("No leader is available")
                                         .build());
            tracing("Leader is not available");
            return;
        } else if (leader.isLocal()) {
            tracing("Processing this request");
            return;
        }

        // redirect the request to leader
        proxyRequestToLeader(requestContext, shardId);
    }

    private boolean processRoutingLoop(ContainerRequestContext requestContext) {
        if (hasRoutingLoop(requestContext)) {
            requestContext.abortWith(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity("Routing loop detected")
                    .build()
            );
            return true;
        }
        return false;
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
     *
     * @param config the config
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

    /**
     * Protected Methods.
     *
     * @param shardId the shard id
     * @return the boolean
     */
    protected boolean isLeaderInShard(String shardId) {
        return gondola.getShard(shardId).getLocalMember().isLeader();
    }

    /**
     * Update bucket range.
     *
     * @param range     the range
     * @param fromShard the from shard
     * @param toShard   the to shard
     */
    protected void updateBucketRange(Range<Integer> range, String fromShard, String toShard,
                                     boolean migrationComplete) {
        bucketManager.updateBucketRange(range, fromShard, toShard, migrationComplete);
    }

    /**
     * Private methods.
     */
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
                BucketManager.ShardState shardState = bucketManager.lookupBucketTable(bucketId);
                // If the shard is migrating and it's local shard, forwarding to migrating shard.
                if (shardState.migratingShardId != null && isMyShard(shardState.shardId)) {
                    return shardState.migratingShardId;
                }
                return shardState.shardId;
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
                tracing("Proxy request to remote server, method={}, URI={}",
                        request.getMethod(), appUri + ((ContainerRequest) request).getRequestUri().getPath());
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
                logger.warn("Unknown error", e);
            }
        }
    }

    /**
     * Block request on buckets.
     *
     * @param splitRange the split range
     */
    public void blockRequestOnBuckets(Range<Integer> splitRange) {
        lockManager.blockRequestOnBuckets(splitRange);
    }

    /**
     * Unblock request on buckets.
     *
     * @param splitRange the split range
     */
    public void unblockRequestOnBuckets(Range<Integer> splitRange) {
        lockManager.unblockRequestOnBuckets(splitRange);
    }

    /**
     * Gets gondola.
     *
     * @return the gondola
     */
    public Gondola getGondola() {
        return gondola;
    }
}
