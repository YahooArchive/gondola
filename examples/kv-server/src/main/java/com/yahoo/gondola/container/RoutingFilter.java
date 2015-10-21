package com.yahoo.gondola.container;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.Member;

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
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RoutingFilter implements Filter {
    /**
     * Routing table
     * Key: memberId, value: HTTP URL
     */
    Map<Integer, String> routingTable = new HashMap<>();
    ClusterCallback callback;

    static Gondola gondola;

    CloseableHttpClient httpclient;
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        httpclient = HttpClients.createDefault();

        String clusterCallbackClass = filterConfig.getInitParameter("clusterCallback");
        if (clusterCallbackClass != null) {
            registerClusterCallback(clusterCallbackClass);
        }
        registerRoutingTable();
    }

    // TODO: move to routing table impl
    private void registerRoutingTable() {
        routingTable.put(81, "http://localhost:8080");
        routingTable.put(82, "http://localhost:8081");
        routingTable.put(83, "http://localhost:8082");
    }

    // TODO: get server scheme://hostname:port
    private String getServerURI() {
        return "http://localhost:8080";
    }


    // TODO: discover
    private void registerClusterCallback(String clusterCallbackClass) throws ServletException {
        try {
            this.callback = (ClusterCallback)RoutingFilter.class.getClassLoader()
                .loadClass(clusterCallbackClass)
                .newInstance();
        } catch (ClassNotFoundException|InstantiationException|IllegalAccessException e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        Member leader = getLeader(request);

        if (isRemoteLeader(leader) && request instanceof HttpServletRequest) {
            proxyRequestToLeader((HttpServletRequest) request, response, leader);
        } else {
            chain.doFilter(request, response);
        }
    }

    private boolean isRemoteLeader(Member leader) {
        return leader != null && !leader.isLocal();
    }

    private Member getLeader(ServletRequest request) {
        if (gondola == null) {
            return null;
        }

        return callback != null ? getLeaderByHelper(request) : getDefaultLeader();
    }

    private Member getLeaderByHelper(ServletRequest request) {
        Member leader;
        String clusterId = callback.getClusterId(request);
        leader = gondola.getCluster(clusterId).getLeader();
        return leader;
    }

    private Member getDefaultLeader() {
        return gondola.getClustersOnHost().get(0).getLeader();
    }

    private void proxyRequestToLeader(HttpServletRequest request, ServletResponse response, Member leader)
        throws IOException {
        String destHost = routingTable.get(leader.getMemberId());
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        String method = request.getMethod();
        String requestURI = request.getRequestURI();
        CloseableHttpResponse proxiedResponse;
        if (method.equals("GET")) {
            HttpGet httpGet = new HttpGet(destHost + requestURI);
            proxiedResponse = httpclient.execute(httpGet);
        } else if (method.equals("PUT")) {
            HttpPut httpPut = new HttpPut(destHost + requestURI);
            httpPut.setHeader(HTTP.CONTENT_TYPE, request.getContentType());
            httpPut.setEntity(new InputStreamEntity(request.getInputStream()));
            proxiedResponse = httpclient.execute(httpPut);
        } else if (method.equals("POST")) {
            HttpPost httpPost = new HttpPost(destHost +requestURI);
            httpPost.setHeader(HTTP.CONTENT_TYPE, request.getContentType());
            httpPost.setEntity(new InputStreamEntity(request.getInputStream()));
            proxiedResponse = httpclient.execute(httpPost);
        } else if (method.equals("DELETE")) {
            HttpDelete httpDelete = new HttpDelete(destHost +requestURI);
            httpDelete.setHeader(HTTP.CONTENT_TYPE, request.getContentType());
            proxiedResponse = httpclient.execute(httpDelete);
        }
        else {
            throw new RuntimeException("Not implemented");
        }

        HttpEntity entity = proxiedResponse.getEntity();
        httpServletResponse.setStatus(proxiedResponse.getStatusLine().getStatusCode());
        httpServletResponse.setHeader("Via", destHost);
        if (entity != null) {
            response.getWriter().write(EntityUtils.toString(entity));
        }
    }

    @Override
    public void destroy() {

    }

    public static void setGondola(Gondola gondola) {
        RoutingFilter.gondola = gondola;
    }
}
