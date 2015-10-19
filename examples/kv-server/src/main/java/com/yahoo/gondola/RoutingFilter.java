package com.yahoo.gondola;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    CloseableHttpClient httpclient;
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        //httpclient = HttpClients.createDefault();
        httpclient = HttpClientBuilder.create().disableContentCompression().build();

        String clusterCallbackClass = filterConfig.getInitParameter("com.yahoo.gondola.ClusterCallback");
        if (clusterCallbackClass != null) {
            registerClusterCallback(clusterCallbackClass);
        }

        // TODO: move to routing table impl
        routingTable.put(81, "http://localhost:8080");
        routingTable.put(82, "http://localhost:8081");
        routingTable.put(83, "http://localhost:8082");
    }

    // TODO: implement this
    private void registerClusterCallback(String clusterCallbackClass) {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        Gondola gondola = GondolaContext.getGondola();

        // Assume one cluster in this host
        Member leader = gondola.getClustersOnHost().get(0).getLeader();
        if (leader != null && !leader.isLocal()) {
            // Only work on HTTP request
            if (request instanceof HttpServletRequest) {
                String destHost = routingTable.get(leader.getMemberId());

                InetSocketAddress address = leader.getAddress();
                HttpServletRequest httpServletRequest = (HttpServletRequest) request;
                HttpServletResponse httpServletResponse = (HttpServletResponse) response;
                String method = httpServletRequest.getMethod();
                String requestURI = httpServletRequest.getRequestURI();
                CloseableHttpResponse proxiedResponse;
                if (method.equals("GET")) {
                    HttpGet httpGet = new HttpGet(destHost + requestURI);
                    proxiedResponse = httpclient.execute(httpGet);
                } else if (method.equals("PUT")) {
                    HttpPut httpPut = new HttpPut(destHost + requestURI);
                    httpPut.setHeader(HTTP.CONTENT_TYPE, httpServletRequest.getContentType());
                    httpPut.setEntity(new InputStreamEntity(httpServletRequest.getInputStream()));
                    proxiedResponse = httpclient.execute(httpPut);
                } else {
                    throw new NotImplementedException();
                }

                HttpEntity entity = proxiedResponse.getEntity();
                httpServletResponse.setStatus(proxiedResponse.getStatusLine().getStatusCode());
                httpServletResponse.setHeader("Via", destHost);
                if (entity != null) {
                    response.getWriter().write(EntityUtils.toString(entity));
                }
            } else {
                chain.doFilter(request, response);
            }
        } else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {

    }


}
