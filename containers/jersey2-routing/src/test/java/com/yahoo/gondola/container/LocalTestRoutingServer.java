/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.spi.RoutingHelper;
import com.yahoo.gondola.container.utils.LocalTestServer;

import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.message.internal.OutboundJaxrsResponse;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response;

/**
 * Integration test harness for testing routing server without a container.
 */
public class LocalTestRoutingServer {

    LocalTestServer localTestServer;
    HttpHost host;
    RoutingFilter routingFilter;

    public LocalTestRoutingServer(Gondola gondola, RoutingHelper routingHelper, ProxyClientProvider proxyClientProvider) throws Exception {
        routingFilter = new RoutingFilter(gondola, routingHelper, proxyClientProvider, null,
                                          null);
        localTestServer = new LocalTestServer((request, response, context) -> {
            try {
                URI requestUri = URI.create(request.getRequestLine().getUri());
                URI
                    baseUri =
                    URI.create(requestUri.getScheme() + "://" + requestUri.getHost() + ":" + requestUri.getPort());
                ContainerRequest
                    containerRequest =
                    new ContainerRequest(baseUri, requestUri, request.getRequestLine().getMethod(), null,
                                         new MapPropertiesDelegate());
                routingFilter.filter(containerRequest);

                Response abortResponse = containerRequest.getAbortResponse();
                Response jaxrsResponse;
                if (abortResponse != null) {
                    jaxrsResponse = abortResponse;
                } else {
                    jaxrsResponse = new OutboundJaxrsResponse.Builder(null).status(200).build();
                }

                ContainerResponse containerResponse = new ContainerResponse(containerRequest, jaxrsResponse);
                routingFilter.filter(containerRequest, containerResponse);
                response.setStatusCode(containerResponse.getStatus());
                response.setEntity(new StringEntity(containerResponse.getEntity().toString()));

                Set<Map.Entry<String, List<Object>>> entries = containerResponse.getHeaders().entrySet();
                for (Map.Entry<String, List<Object>> e : entries) {
                    String headerName = e.getKey();
                    for (Object o : e.getValue()) {
                        String headerValue = o.toString();
                        response.setHeader(headerName, headerValue);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        });
        host = localTestServer.start();
    }

    public String getHostUri() {
        return host.getSchemeName() + "://" + host.getHostName() + ":" + host.getPort();
    }
}
