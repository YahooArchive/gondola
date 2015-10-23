package com.yahoo.gondola.container;

import com.yahoo.gondola.Cluster;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RoutingFilter implements Filter {

    public static final String X_GONDOLA_LEADER_ADDRESS = "X-Gondola-Leader-Address";
    /**
     * Routing table
     * Key: memberId, value: HTTP URL
     */
    ClusterCallback callback;

    static Gondola gondola;
    FilterConfig filterConfig;
    Set<String> myClusterIds;
    Map<String, List<String>> routingTable;

    CloseableHttpClient httpclient;
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        httpclient = HttpClients.createDefault();
        this.filterConfig = filterConfig;

        registerCallback();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        // Only process http servlet request
        if (!(request instanceof HttpServletRequest)) {
            chain.doFilter(request, response);
            return;
        }

        loadRoutingTableIfNeeded();
        String clusterId = getClusterId(request);
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;

        if (isMyCluster(clusterId)) {
            Member leader = getLeader(clusterId);

            // Those are the condition that the server should process this request
            // Still under leader election
            if (leader == null) {
                ((HttpServletResponse) response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
            } else if (leader.isLocal()) {
                chain.doFilter(request, response);
                return;
            }
        }

        // Proxy the request to other server
        proxyRequestToLeader(httpServletRequest, response, clusterId);
    }

    @Override
    public void destroy() {

    }

    /**
     * Set Gondola instance explicitly from WebApp
     * @param gondola Gondola instance
     */
    public static void setGondola(Gondola gondola) {
        RoutingFilter.gondola = gondola;
    }

    private void registerCallback() throws ServletException {
        String clusterCallbackClass = filterConfig.getInitParameter("clusterCallback");
        if (clusterCallbackClass != null) {
            try {
                callback = (ClusterCallback) RoutingFilter.class.getClassLoader()
                    .loadClass(clusterCallbackClass)
                    .newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new ServletException(e);
            }
        }
    }

    // TODO: load from gondola config, require the App port present in gondola config
    private void loadRoutingTableIfNeeded() {
        if (routingTable == null) {
            ConcurrentHashMap<String, List<String>> newRoutingTable = new ConcurrentHashMap<>();
            Config config = gondola.getConfig();
            for(String hostId : config.getHostIds()) {
                for(String clusterId : config.getClusterIds(hostId)) {
                    InetSocketAddress address = config.getAddressForHost(hostId);
                    List<String> addresses = newRoutingTable.get(clusterId);
                    if (addresses == null) {
                        addresses = new ArrayList<>();
                        newRoutingTable.put(clusterId, addresses);
                    }
                    // TODO: construct address in the form of : <scheme>://<hostname>:<port>
                    addresses.add(address.getHostName());
                }
            }
            routingTable = newRoutingTable;
        }
    }


    private String getClusterId(ServletRequest request) {
        if (callback != null) {
            return callback.getClusterId(gondola, request);
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

    private void proxyRequestToLeader(HttpServletRequest request, ServletResponse response, String clusterId) {
        List<String> appUrls = lookupRoutingTable(clusterId);
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;

        for(String appUrl : appUrls) {
            try (CloseableHttpResponse proxiedResponse = proxyRequest(appUrl, request)) {
                HttpEntity entity = proxiedResponse.getEntity();
                httpServletResponse.setStatus(proxiedResponse.getStatusLine().getStatusCode());
                httpServletResponse.setHeader(X_GONDOLA_LEADER_ADDRESS, appUrl);
                if (entity != null) {
                    response.getWriter().write(EntityUtils.toString(entity));
                }
                updateRoutingTableIfNeeded(clusterId, proxiedResponse);
                return;
            } catch (IOException ignored) {
                // continue
            }
        }
        ((HttpServletResponse) response).setStatus(HttpServletResponse.SC_BAD_GATEWAY);
    }

    private void updateRoutingTableIfNeeded(String clusterId, CloseableHttpResponse proxiedResponse) {
        Header[] headers = proxiedResponse.getHeaders(X_GONDOLA_LEADER_ADDRESS);
        if (headers != null) {
            updateRoutingTable(clusterId, headers[0].getValue());
        }
    }

    private void updateRoutingTable(String clusterId, String newAppUrl) {
        List<String> appUrls = lookupRoutingTable(clusterId);
        List<String> newAppUrls = new ArrayList<>(appUrls.size());
        newAppUrls.add(newAppUrl);
        for(String appUrl : appUrls) {
            if (!appUrl.equals(newAppUrl)) {
                newAppUrls.add(appUrl);
            }
        }
        routingTable.put(clusterId, newAppUrls);
    }

    /**
     * Lookup leader App URL in routing table
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
     * @param appUrl The target App URL
     * @param request The original request
     * @return the response of the proxied request
     * @throws IOException
     */
    private CloseableHttpResponse proxyRequest(String appUrl, HttpServletRequest request) throws IOException {
        String method = request.getMethod();
        String requestURI = request.getRequestURI();
        CloseableHttpResponse proxiedResponse;
        switch (method) {
            case "GET":
                HttpGet httpGet = new HttpGet(appUrl + requestURI);
                proxiedResponse = httpclient.execute(httpGet);
                break;
            case "PUT":
                HttpPut httpPut = new HttpPut(appUrl + requestURI);
                httpPut.setHeader(HTTP.CONTENT_TYPE, request.getContentType());
                httpPut.setEntity(new InputStreamEntity(request.getInputStream()));
                proxiedResponse = httpclient.execute(httpPut);
                break;
            case "POST":
                HttpPost httpPost = new HttpPost(appUrl + requestURI);
                httpPost.setHeader(HTTP.CONTENT_TYPE, request.getContentType());
                httpPost.setEntity(new InputStreamEntity(request.getInputStream()));
                proxiedResponse = httpclient.execute(httpPost);
                break;
            case "DELETE":
                HttpDelete httpDelete = new HttpDelete(appUrl + requestURI);
                httpDelete.setHeader(HTTP.CONTENT_TYPE, request.getContentType());
                proxiedResponse = httpclient.execute(httpDelete);
                break;
            default:
                throw new RuntimeException("Method not supported: " + method);
        }
        return proxiedResponse;
    }


}
