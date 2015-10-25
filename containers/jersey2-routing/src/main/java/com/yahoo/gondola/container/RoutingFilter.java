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

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;


/**
 * RoutingFilter is a Jersey2 compatible routing filter that provides routing request to leader host before hitting the
 * resource.
 */
public class RoutingFilter implements ContainerRequestFilter {

    public static final String X_GONDOLA_LEADER_ADDRESS = "X-Gondola-Leader-Address";
    public static final String APP_PORT = "appPort";
    public static final String APP_SCHEME = "appScheme";
    /**
     * Routing table Key: memberId, value: HTTP URL
     */
    ClusterIdCallback clusterIdCallback;

    Gondola gondola;
    Set<String> myClusterIds;

    // clusterId --> list of available servers
    Map<String, List<String>> routingTable;

    CloseableHttpClient httpclient;

    public RoutingFilter(Gondola gondola, ClusterIdCallback clusterIdCallback) throws ServletException {
        this.gondola = gondola;
        this.clusterIdCallback = clusterIdCallback;
        httpclient = HttpClients.createDefault();
        loadRoutingTable();
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String clusterId = getClusterId(requestContext);

        if (isMyCluster(clusterId)) {
            Member leader = getLeader(clusterId);

            // Those are the condition that the server should process this request
            // Still under leader election
            if (leader == null) {
                requestContext.abortWith(Response
                                             .status(Response.Status.INTERNAL_SERVER_ERROR)
                                             .entity("Under leader election")
                                             .build());
                return;
            } else if (leader.isLocal()) {
                return;
            }
        }

        // Proxy the request to other server
        proxyRequestToLeader(requestContext, clusterId);
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
        if (clusterIdCallback != null) {
            return clusterIdCallback.getClusterId(request);
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

    private void proxyRequestToLeader(ContainerRequestContext request, String clusterId) {
        List<String> appUrls = lookupRoutingTable(clusterId);

        for (String appUrl : appUrls) {
            try (CloseableHttpResponse proxiedResponse = proxyRequest(appUrl, request)) {
                String
                    entity =
                    proxiedResponse.getEntity() != null ? EntityUtils.toString(proxiedResponse.getEntity()) : "";
                request.abortWith(Response
                                      .status(proxiedResponse.getStatusLine().getStatusCode())
                                      .entity(entity)
                                      .header(X_GONDOLA_LEADER_ADDRESS, appUrl)
                                      .build());
                updateRoutingTableIfNeeded(clusterId, proxiedResponse);
                return;
            } catch (IOException ignored) {
                ignored.printStackTrace();
            }
        }
        request.abortWith(Response
                              .status(Response.Status.BAD_GATEWAY)
                              .entity("All servers are not available in Cluster: " + clusterId)
                              .build());
    }

    private void updateRoutingTableIfNeeded(String clusterId, CloseableHttpResponse proxiedResponse) {
        Header[] headers = proxiedResponse.getHeaders(X_GONDOLA_LEADER_ADDRESS);
        if (headers.length > 0) {
            setClusterLeader(clusterId, headers[0].getValue());
        }
    }

    /**
     * Move newAppUrl to the first entry of the routing table.
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
     * Lookup leader App URL in routing table
     *
     * @param clusterId The Gondola clusterId
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
     * proxy request to destination host
     *
     * @param appUrl  The target App URL
     * @param request The original request
     * @return the response of the proxied request
     */
    private CloseableHttpResponse proxyRequest(String appUrl, ContainerRequestContext request) throws IOException {
        String method = request.getMethod();
        String requestURI = request.getUriInfo().getRequestUri().getPath();
        CloseableHttpResponse proxiedResponse;
        switch (method) {
            case "GET":
                HttpGet httpGet = new HttpGet(appUrl + requestURI);
                proxiedResponse = httpclient.execute(httpGet);
                break;
            case "PUT":
                HttpPut httpPut = new HttpPut(appUrl + requestURI);
                httpPut.setHeader(HTTP.CONTENT_TYPE, request.getHeaderString("Content-Type"));
                httpPut.setEntity(new InputStreamEntity(request.getEntityStream()));
                proxiedResponse = httpclient.execute(httpPut);
                break;
            case "POST":
                HttpPost httpPost = new HttpPost(appUrl + requestURI);
                httpPost.setHeader(HTTP.CONTENT_TYPE, request.getHeaderString("Content-Type"));
                httpPost.setEntity(new InputStreamEntity(request.getEntityStream()));
                proxiedResponse = httpclient.execute(httpPost);
                break;
            case "DELETE":
                HttpDelete httpDelete = new HttpDelete(appUrl + requestURI);
                httpDelete.setHeader(HTTP.CONTENT_TYPE, request.getHeaderString("Content-Type"));
                proxiedResponse = httpclient.execute(httpDelete);
                break;
            default:
                throw new IllegalStateException("Method not supported: " + method);
        }
        return proxiedResponse;
    }
}
