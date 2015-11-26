/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_GATEWAY;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;


/**
 * RoutingFilter is a Jersey2 compatible routing filter that provides routing request to leader host before hitting the
 * resource.
 */
public class RoutingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    public static final int POLLING_TIMES = 3;
    private static Logger logger = LoggerFactory.getLogger(RoutingFilter.class);

    static final String X_GONDOLA_LEADER_ADDRESS = "X-Gondola-Leader-Address";
    static final String X_FORWARDED_BY = "X-Forwarded-By";
    static final String APP_PORT = "appPort";
    static final String APP_SCHEME = "appScheme";

    private RoutingHelper routingHelper;
    private Gondola gondola;
    private Set<String> myShardIds;

    // shardId --> list of available servers. (URI)
    private Map<String, List<String>> routingTable;

    private Map<Integer, AtomicInteger> bucketRequestCounters = new ConcurrentHashMap<>();
    private ProxyClient proxyClient;

    // Serialized command executor.
    private ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    private LockManager lockManager;

    private Map<String, String> serviceUris = new HashMap<>();

    private boolean tracing = false;
    private String myAppUri;
    private BucketManager bucketManager;

    private List<Runnable> shutdownCallbacks = new ArrayList<>();

    private static List<RoutingFilter> instances = new ArrayList<>();
    private ResourceConfig application;

    /**
     * Disallow default constructor.
     */
    private RoutingFilter() {
    }

    /**
     * Get application instance.
     *
     * @return Application instance.
     */
    public ResourceConfig getApplication() {
        return application;
    }

    /**
     * Instantiates a new Routing filter.
     *
     * @param gondola             the gondola
     * @param routingHelper       the routing helper
     * @param proxyClientProvider the proxy client provider
     * @param application         the application instance
     * @throws ServletException the servlet exception
     */
    RoutingFilter(Gondola gondola, RoutingHelper routingHelper, ProxyClientProvider proxyClientProvider,
                  ResourceConfig application)
        throws ServletException {
        this.gondola = gondola;
        this.routingHelper = routingHelper;
        this.application = application;
        lockManager = new LockManager(gondola.getConfig());
        bucketManager = new BucketManager(gondola.getConfig());
        loadRoutingTable();
        loadConfig();
        watchGondolaEvent();
        proxyClient = proxyClientProvider.getProxyClient(gondola.getConfig());
        instances.add(this);
    }

    private void loadConfig() {
        gondola.getConfig().registerForUpdates(config -> tracing = config.getBoolean("tracing.router"));
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
                        trace("[{}] memberId={} Become leader on \"{}\", blocking all requests to the shard....", gondola.getHostId(), roleChangeEvent.leader.getMemberId(), shardId);
                        lockManager.blockRequestOnShard(shardId);
                        trace("[{}] Wait until raft logs applied to storage...", gondola.getHostId());
                        waitDrainRaftLogs(shardId);
                        trace("[{}] Raft logs are up-to-date, notify application is ready to serve...", gondola.getHostId());
                        routingHelper.beforeServing(shardId);
                        trace("[{}] Ready for serving, unblocking the requests...", gondola.getHostId());
                        long count = lockManager.unblockRequestOnShard(shardId);
                        trace("[{}] System is back to serving, unblocked {} requests ...", gondola.getHostId(), count);
                    }, singleThreadExecutor).exceptionally(throwable -> {
                        logger.info("[{}] Errors while executing leader change event. message={}", gondola.getHostId(), throwable.getMessage());
                        return null;
                    });
                }
            }
        });
    }

    private void trace(String format, Object... args) {
        if (tracing) {
            logger.info(format, args);
        }
    }


    @Override
    public void filter(ContainerRequestContext request) throws IOException {
        // Extract required data
        int bucketId = routingHelper.getBucketId(request);
        incrementBucketCounter(bucketId);
        String shardId = getShardId(request);

        trace("Processing request: {} of shard={}, forwarded={}",
              request.getUriInfo().getAbsolutePath(), shardId,
              request.getHeaders().containsKey(X_FORWARDED_BY) ? request.getHeaders()
                  .get(X_FORWARDED_BY).toString() : "");

        if (hasRoutingLoop(request)) {
            abortResponse(request, BAD_REQUEST, "Routing loop detected");
            return;
        }

        if (shardId == null) {
            abortResponse(request, BAD_REQUEST, "Cannot find shard for bucketId=" + bucketId);
            return;
        }

        // redirect the request to other shard
        if (!isMyShard(shardId)) {
            proxyRequestToLeader(request, shardId);
            return;
        }

        // Block request if needed
        blockRequest(bucketId, shardId);

        Member leader = getLeader(shardId);
        // Those are the condition that the server should process this request
        // Still under leader election
        if (leader == null) {
            abortResponse(request, SERVICE_UNAVAILABLE, "No leader is available");
            trace("Leader is not available");
            return;
        } else if (leader.isLocal()) {
            trace("Processing this request");
            return;
        }

        // redirect the request to leader
        proxyRequestToLeader(request, shardId);
    }

    private void abortResponse(ContainerRequestContext requestContext, Response.Status status, String stringEntity) {
        requestContext.abortWith(
            Response.status(status)
                .entity(stringEntity)
                .build()
        );
    }

    private void blockRequest(int bucketId, String shardId) throws IOException {
        try {
            lockManager.filterRequest(bucketId, shardId);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
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
                sb.append("Shard bucketMap attribute is missing on Shard - ").append(shardId).append("\n");
            }
        }

        for (String hostId : config.getHostIds()) {
            Map<String, String> attributes = config.getAttributesForHost(hostId);
            if (!attributes.containsKey("appScheme") || !attributes.containsKey("appPort")) {
                sb.append("Host attributes appScheme and appPort is missing on Host - ").append(hostId).append("\n");
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

    protected boolean isBucketRange(Range<Integer> range, String shardId, String migratingShardId) {
        BucketManager.ShardState shardState = bucketManager.lookupBucketTable(range);
        return shardState.shardId.equals(shardId) && shardState.migratingShardId.equals(migratingShardId);
    }


    /**
     * Waits until there is no request to the target buckets.
     *
     * @return true if no requests on buckets, false if timeout.
     */
    protected boolean waitNoRequestsOnBuckets(Range<Integer> splitRange, long timeoutMs)
        throws InterruptedException, ExecutionException {

        trace("Waiting for no requests on buckets: {} with timeout={}ms, current requestCount={}",
              splitRange, timeoutMs, getRequestCount(splitRange));
        return Utils.pollingWithTimeout(() -> {
            long requestCount = getRequestCount(splitRange);
            if (requestCount != 0) {
                trace("Waiting for no requests on buckets: {} with timeout={}ms, current requestCount={}",
                      splitRange, timeoutMs, getRequestCount(splitRange));
                return false;
            }
            return true;
        }, timeoutMs / POLLING_TIMES, timeoutMs);
    }

    private long getRequestCount(Range<Integer> splitRange) {
        long requestCount = 0;
        for (int i = splitRange.lowerEndpoint(); i <= splitRange.upperEndpoint(); i++) {
            if (bucketRequestCounters.containsKey(i)) {
                requestCount += bucketRequestCounters.get(i).get();
            }
        }
        return requestCount;
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
                trace("Proxy request to remote server, method={}, URI={}",
                      request.getMethod(), appUri + request.getUriInfo().getRequestUri());
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
        abortResponse(request, BAD_GATEWAY, "All servers are not available in Shard: " + shardId);
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
        Config config = gondola.getConfig();
        return config.getHostIds().stream()
            .map(config::getSiteIdForHost)
            .filter(s -> s.equals(siteId))
            .findAny().orElseThrow(() -> new IllegalStateException("Cannot find any shard in siteId=" + siteId));
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

    private void waitDrainRaftLogs(String shardId) {
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


    public void registerShutdownFunction(Runnable runnable) {
        shutdownCallbacks.add(runnable);
    }

    private void stop() {
        shutdownCallbacks.forEach(Runnable::run);
        gondola.stop();
    }

    /**
     * Builder class.
     */
    public static class Builder {

        Gondola gondola;
        RoutingHelper routingHelper;
        ProxyClientProvider proxyClientProvider;
        ShardManagerProvider shardManagerProvider;
        ResourceConfig application;

        public static Builder createRoutingFilter() {
            return new Builder();
        }

        Builder() {
            proxyClientProvider = new ProxyClientProvider();
        }

        public Builder setGondola(Gondola gondola) {
            this.gondola = gondola;
            return this;
        }

        public Builder setRoutingHelper(RoutingHelper routingHelper) {
            this.routingHelper = routingHelper;
            return this;
        }

        public Builder setProxyClientProvider(ProxyClientProvider proxyClientProvider) {
            this.proxyClientProvider = proxyClientProvider;
            return this;
        }

        public Builder setShardManagerProvider(ShardManagerProvider shardManagerProvider) {
            this.shardManagerProvider = shardManagerProvider;
            return this;
        }

        public Builder setApplication(ResourceConfig application) {
            this.application = application;
            return this;
        }

        public RoutingFilter build() throws ServletException {
            Preconditions.checkState(gondola != null, "Gondola instance must be set");
            Preconditions.checkState(routingHelper != null, "RoutingHelper instance must be set");
            Preconditions.checkState(application != null, "ResourceConfig instance must be set");
            if (shardManagerProvider == null) {
                shardManagerProvider = new ShardManagerProvider(gondola.getConfig());
            }
            RoutingFilter routingFilter = new RoutingFilter(gondola, routingHelper, proxyClientProvider, application);
            initializeShardManagerServer(routingFilter);
            return routingFilter;
        }

        private void initializeShardManagerServer(RoutingFilter routingFilter) {
            ShardManagerServer shardManagerServer = shardManagerProvider.getShardManagerServer();
            ShardManager shardManager =
                new ShardManager(gondola, routingFilter, gondola.getConfig(),
                                 shardManagerProvider.getShardManagerClient());
            shardManagerServer.setShardManager(shardManager);
            routingFilter.registerShutdownFunction(shardManagerServer::stop);
        }
    }

    public static List<RoutingFilter> getInstance() {
        return instances;
    }

    /**
     * WebApp context listener.
     */
    @WebListener
    public static class ContextListener implements ServletContextListener {

        @Override
        public void contextInitialized(ServletContextEvent sce) {
        }

        private int discoveryJersey9Port(ServletContextEvent sce) {
            try {
                ServletContext servletContext = sce.getServletContext();
                Field webAppContextField = servletContext.getClass().getDeclaredField("this$0");
                webAppContextField.setAccessible(true);
                Object webAppContext = webAppContextField.get(servletContext);
                Object server = webAppContext.getClass().getMethod("getServer").invoke(webAppContext);
                Object connector =
                    ((Object[]) server.getClass().getMethod("getConnectors").invoke(server))[0];
                return (int) connector.getClass().getDeclaredMethod("getPort").invoke(connector);
            } catch (NoSuchFieldException | NoSuchMethodException
                | InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException("Extract information from Jetty9 failed...");
            }
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            RoutingFilter.getInstance().forEach(RoutingFilter::stop);
            RoutingFilter.getInstance().clear();
        }
    }
}
