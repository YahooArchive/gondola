/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.codahale.metrics.Timer;
import com.google.common.collect.Range;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.client.ProxyClient;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
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
    public static final int RETRY_TIMES = 3;
    public static final int WAIT_MS = 1000;
    private static Logger logger = LoggerFactory.getLogger(RoutingFilter.class);

    public static final String X_GONDOLA_LEADER_ADDRESS = "X-Gondola-Leader-Address";
    public static final String X_FORWARDED_BY = "X-Forwarded-By";
    public static final String X_GONDOLA_ERROR = "X-Gondola-Error";
    public static final String X_GONDOLA_BUCKET_ID = "X-Gondola-Bucket-Id";
    public static final String X_GONDOLA_SHARD_ID = "X-Gondola-Shard-Id";

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
    private ChangeLogProcessor changeLogProcessor;
    private Map<String, RoutingService> services;
    private Pattern whiteList = Pattern.compile("^gondola/.*");

    private ReentrantLock lock = new ReentrantLock();
    Condition leaderFoundCondition = lock.newCondition();

    private Timer forwardTimer;
    private Timer processTimer;
    private Timer errorTimer;

    /**
     * Disallow default constructor.
     */
    private RoutingFilter() {
    }

    /**
     * Instantiates a new Routing filter.
     *
     * @param gondola             the gondola
     * @param proxyClientProvider the proxy client provider
     * @throws ServletException the servlet exception
     */
    RoutingFilter(Gondola gondola, RoutingHelper routingHelper, ProxyClientProvider proxyClientProvider,
                  Map<String, RoutingService> services, ChangeLogProcessor changeLogProcessor)
        throws ServletException {
        this.gondola = gondola;
        lockManager = new LockManager(gondola);
        bucketManager = new BucketManager(gondola.getConfig());
        loadRoutingTable();
        loadConfig();
        watchGondolaEvent();
        proxyClient = proxyClientProvider.getProxyClient(gondola.getConfig());
        instances.add(this);
        this.services = services;
        this.routingHelper = routingHelper;
        this.changeLogProcessor = changeLogProcessor;
        forwardTimer = GondolaApplication.MyMetricsServletContextListener.METRIC_REGISTRY.timer("filter.forward-timer");
        processTimer = GondolaApplication.MyMetricsServletContextListener.METRIC_REGISTRY.timer("filter.process-timer");
        errorTimer = GondolaApplication.MyMetricsServletContextListener.METRIC_REGISTRY.timer("filter.error-timer");
    }

    /**
     * Called by RoutingService factory. to get the right service.
     */
    public RoutingService getService(ContainerRequestContext request) {
        extractShardAndBucketIdFromRequest(request);
        return services.get(getShardIdFromRequest(request));
    }

    private void loadConfig() {
        gondola.getConfig().registerForUpdates(config -> tracing = config.getBoolean("tracing.router"));
    }

    private void watchGondolaEvent() {
        gondola.registerForRoleChanges(roleChangeEvent -> {
            if (roleChangeEvent.leader != null) {
                lock.lock();
                try {
                    logger
                        .info("[{}] New leader {} elected.", gondola.getHostId(), roleChangeEvent.leader.getMemberId());
                    leaderFoundCondition.signalAll();
                } finally {
                    lock.unlock();
                }
                Config config = gondola.getConfig();
                String
                    appUri =
                    Utils.getAppUri(config, config.getMember(roleChangeEvent.leader.getMemberId()).getHostId());
                updateShardRoutingEntries(roleChangeEvent.shard.getShardId(), appUri);
                if (roleChangeEvent.leader.isLocal()) {
                    CompletableFuture.runAsync(() -> {
                        String shardId = roleChangeEvent.shard.getShardId();
                        trace("[{}-{}] Become leader on \"{}\", blocking all requests to the shard....",
                              gondola.getHostId(), roleChangeEvent.leader.getMemberId(), shardId);
                        lockManager.blockRequestOnShard(shardId);
                        trace("[{}-{}] Wait until raft logs applied to storage...",
                              gondola.getHostId(), roleChangeEvent.leader.getMemberId());
                        waitDrainRaftLogs(shardId);
                        trace("[{}-{}] Raft logs are up-to-date, notify application is ready to serve...",
                              gondola.getHostId(), roleChangeEvent.leader.getMemberId());
                        services.get(roleChangeEvent.shard.getShardId()).ready();
                        trace("[{}-{}] Ready for serving, unblocking the requests...",
                              gondola.getHostId(), roleChangeEvent.leader.getMemberId());
                        long count = lockManager.unblockRequestOnShard(shardId);
                        trace("[{}-{}] System is back to serving, unblocked {} requests ...",
                              gondola.getHostId(), roleChangeEvent.leader.getMemberId(), count);
                    }, singleThreadExecutor).exceptionally(throwable -> {
                        logger.info("[{}-{}] Errors while executing leader change event. message={}",
                                    gondola.getHostId(), roleChangeEvent.leader.getMemberId(), throwable.getMessage());
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


    public void extractShardAndBucketIdFromRequest(ContainerRequestContext request) {
        // Extract required data
        if (request.getProperty("shardId") == null) {
            int bucketId = getBucketId(request);
            String shardId = getShardId(bucketId);

            request.setProperty("shardId", shardId);
            request.setProperty("bucketId", bucketId);
        }
    }

    private int getBucketIdFromRequest(ContainerRequestContext request) {
        return (Integer) request.getProperty("bucketId");
    }

    private String getShardIdFromRequest(ContainerRequestContext request) {
        return (String) request.getProperty("shardId");
    }


    private int getBucketId(ContainerRequestContext request) {
        return Math.abs(routingHelper.getBucketHash(request) % bucketManager.getNumberOfBuckets());
    }


    @Override
    public void filter(ContainerRequestContext request) throws IOException {
        if (isWhiteList(request)) {
            request.setProperty("whiteList", true);
            return;
        }
        extractShardAndBucketIdFromRequest(request);
        int bucketId = getBucketIdFromRequest(request);
        String shardId = getShardIdFromRequest(request);

        incrementBucketCounter(bucketId);
        trace("Processing request: {} of shard={}, forwarded={}",
              request.getUriInfo().getAbsolutePath(), shardId,
              request.getHeaders().containsKey(X_FORWARDED_BY) ? request.getHeaders()
                  .get(X_FORWARDED_BY).toString() : "");

        if (hasRoutingLoop(request)) {
            abortResponse(request, BAD_REQUEST, "Routing loop detected");
            request.setProperty("timer", errorTimer.time());
            return;
        }

        if (shardId == null) {
            abortResponse(request, BAD_REQUEST, "Cannot find shard for bucketId=" + bucketId);
            request.setProperty("timer", errorTimer.time());
            return;
        }

        // redirect the request to other shard
        if (!isMyShard(shardId)) {
            proxyRequestToLeader(request, shardId);
            request.setProperty("timer", forwardTimer.time());
            return;
        }

        // Block request if needed
        blockRequest(bucketId, shardId);

        Member leader = null;
        try {
            leader = waitForLeader(shardId);
        } catch (InterruptedException e) {
            abortResponse(request, Response.Status.INTERNAL_SERVER_ERROR, "Request interrupted");
            request.setProperty("timer", errorTimer.time());
            return;
        }

        if (leader == null) {
            abortResponse(request, SERVICE_UNAVAILABLE, "No leader is available");
            request.setProperty("timer", errorTimer.time());
            return;

        }

        if (leader.isLocal()) {
            trace("Processing this request");
            request.setProperty("timer", processTimer.time());
            return;
        }

        // redirect the request to leader
        proxyRequestToLeader(request, shardId);
        request.setProperty("timer", forwardTimer.time());
    }

    private Member waitForLeader(String shardId) throws InterruptedException {
        Member leader;
        lock.lock();
        try {
            for (int i = 0; i < RETRY_TIMES; i++) {
                leader = getLeader(shardId);
                if (leader != null) {
                    return leader;
                }
                leaderFoundCondition.await(WAIT_MS, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    private boolean isWhiteList(ContainerRequestContext request) {
        return whiteList.matcher(request.getUriInfo().getPath()).matches();
    }

    /**
     * The function is used for tell other routing filter that this server is not able to handle the request.
     */
    private void abortResponse(ContainerRequestContext requestContext, Response.Status status, String stringEntity) {
        requestContext.abortWith(
            Response.status(status)
                .entity(stringEntity)
                .header(X_GONDOLA_ERROR, stringEntity)
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
        if (requestContext.getProperty("whiteList") != null) {
            return;
        }
        decrementBucketCounter((Integer) requestContext.getProperty("bucketId"));
        responseContext.getHeaders()
            .put(X_GONDOLA_BUCKET_ID, Collections.singletonList(requestContext.getProperty("bucketId")));
        responseContext.getHeaders()
            .put(X_GONDOLA_SHARD_ID, Collections.singletonList(requestContext.getProperty("shardId")));
        if (requestContext.getProperty("timer") != null) {
            ((Timer.Context) requestContext.getProperty("timer")).stop();
        }
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
        trace("[{}] Update bucket range={} from {} to {}",
              gondola.getHostId(), range, fromShard, toShard);
        bucketManager.updateBucketRange(range, fromShard, toShard, migrationComplete);
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
                myAppUri = Utils.getAppUri(config, hostId);
                continue;
            }
            String appUri = Utils.getAppUri(config, hostId);
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


    private String getShardId(int bucketId) {
        if (bucketId == -1) {
            return getAffinityColoShard();
        } else {
            BucketManager.ShardState shardState = bucketManager.lookupBucketTable(bucketId);
            // If the shard is migrating and it's local shard, forwarding to migrating shard.
            if (shardState.migratingShardId != null && isMyShard(shardState.shardId)) {
                return shardState.migratingShardId;
            }
            return shardState.shardId;
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

        boolean failed = false;
        for (String appUri : appUris) {
            for (int i = 0; i < RETRY_TIMES; i++) {
                try {
                    trace("Proxy request to remote server, method={}, URI={}",
                          request.getMethod(), appUri + request.getUriInfo().getRequestUri());

                    if (sendProxyRequest(request, shardId, appUri)) {
                        // If the remote server is the leader, and the local server routing table is not right,
                        // update routing table with the request appUri.
                        if (failed) {
                            updateShardRoutingEntries(shardId, appUri);
                        }
                        return;
                    }
                    failed = true;
                    break;
                } catch (IOException e) {
                    logger.warn("[{}] Error while forwarding request to shard:{} {}, message={}",
                                gondola.getHostId(), shardId, appUri, e.getMessage());
                }
            }
        }
        abortResponse(request, BAD_GATEWAY, "All servers are not available in Shard: " + shardId);
    }

    private boolean sendProxyRequest(ContainerRequestContext request, String shardId, String appUri)
        throws IOException {

        appendForwardedByHeader(request);

        Response proxiedResponse = proxyClient.proxyRequest(request, appUri);

        try {
            // Remote server is not able to serve the request.
            if (proxiedResponse.getHeaderString(X_GONDOLA_ERROR) != null) {
                return false;
            }

            String entity = getResponseEntity(proxiedResponse);

            // Proxied response successful, response the data to the requester.
            request.abortWith(Response
                                  .status(proxiedResponse.getStatus())
                                  .entity(entity)
                                  .header(X_GONDOLA_LEADER_ADDRESS, appUri)
                                  .build());

            // If remote server is not the leader (2-hop, update the local routing table.
            updateRoutingTableIfNeeded(shardId, proxiedResponse);
            return true;
        } finally {
            proxiedResponse.close();
        }
    }


    private String getResponseEntity(Response proxiedResponse) {
        return proxiedResponse.getEntity() != null ? proxiedResponse.getEntity().toString() : "";
    }

    private void appendForwardedByHeader(ContainerRequestContext request) {
        List<String> forwardedBy = request.getHeaders().get(X_FORWARDED_BY);

        if (forwardedBy == null) {
            forwardedBy = Collections.singletonList(myAppUri);
        } else {
            forwardedBy = Collections.singletonList(forwardedBy.get(0) + "," + myAppUri);
        }
        request.getHeaders().put(X_FORWARDED_BY, forwardedBy);
    }

    private void updateRoutingTableIfNeeded(String shardId, Response proxiedResponse) {
        String appUri = proxiedResponse.getHeaderString(X_GONDOLA_LEADER_ADDRESS);
        if (appUri != null) {
            logger.info("[{}] New leader found, correct routing table with : shardId={}, appUrl={}",
                        gondola.getHostId(), shardId, appUri);
            updateShardRoutingEntries(shardId, appUri);
        }
    }

    /**
     * Moves newAppUrl to the first entry of the routing table.
     */
    private void updateShardRoutingEntries(String shardId, String appUri) {
        if (appUri.equals(myAppUri)) {
            logger.warn("Trying to update routingTable with self appUri. -- skip");
            return;
        }
        List<String> appUris = lookupRoutingTable(shardId);
        List<String> newAppUris = new ArrayList<>(appUris.size());
        newAppUris.add(appUri);
        for (String appUrl : appUris) {
            if (!appUrl.equals(appUri)) {
                newAppUris.add(appUrl);
            }
        }
        trace("[{}] Update shard '{}' leader as {}", gondola.getHostId(), shardId, appUri);
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

    private String getAffinityColoShard() {
        String siteId = getSiteId();
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
                int commitIndex = gondola.getShard(shardId).getCommitIndex();
                int appliedIndex = changeLogProcessor.getAppliedIndex(shardId);
                int diff = commitIndex - appliedIndex;
                if (now - checkTime > 10000) {
                    checkTime = now;
                    logger.warn("[{}] Recovery running for {} seconds, {} logs left, ci={}, ai={}", gondola.getHostId(),
                                (now - startTime) / 1000, diff, commitIndex, appliedIndex);
                }
                synced = diff <= 0;
            } catch (Exception e) {
                logger.warn("[{}] Unknown error. message={}", gondola.getHostId(), e);
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

    public void start() {
        changeLogProcessor.start();
    }

    private void stop() {
        changeLogProcessor.stop();
        shutdownCallbacks.forEach(Runnable::run);
        gondola.stop();
    }

    public String getSiteId() {
        return gondola.getConfig().getSiteIdForHost(gondola.getHostId());
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

    public Gondola getGondola() {
        return gondola;
    }

    public ChangeLogProcessor getChangeLogProcessor() {
        return changeLogProcessor;
    }

    public BucketManager getBucketManager() {
        return bucketManager;
    }

    public Map<String, List<String>> getRoutingTable() {
        return routingTable;
    }

    public LockManager getLockManager() {
        return lockManager;
    }
}
