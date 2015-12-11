/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Proxy client implementation using Apache Http Client.
 */
public class ApacheHttpComponentProxyClient implements ProxyClient {

    /**
     * The Httpclient.
     */
    CloseableHttpClient httpClient;

    public ApacheHttpComponentProxyClient() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(200);
        cm.setDefaultMaxPerRoute(200);
        httpClient = HttpClients.custom()
            .setConnectionManager(cm)
            .build();
    }

    /**
     * Proxies the request to the destination host.
     *
     * @param request The original request
     * @param baseUri The target App URL
     * @return the response of the proxied request
     */
    @Override
    public Response proxyRequest(ContainerRequestContext request, String baseUri) throws IOException {
        String method = request.getMethod();
        String requestURI = request.getUriInfo().getRequestUri().getPath();
        CloseableHttpResponse proxiedResponse;
        switch (method) {
            case "GET":
                proxiedResponse = executeRequest(new HttpGet(baseUri + requestURI), request);
                break;
            case "PUT":
                proxiedResponse = executeRequest(new HttpPut(baseUri + requestURI), request);
                break;
            case "POST":
                proxiedResponse = executeRequest(new HttpPost(baseUri + requestURI), request);
                break;
            case "DELETE":
                proxiedResponse = executeRequest(new HttpDelete(baseUri + requestURI), request);
                break;
            case "PATCH":
                proxiedResponse = executeRequest(new HttpPatch(baseUri + requestURI), request);
                break;
            default:
                throw new IllegalStateException("Method not supported: " + method);
        }

        return getResponse(proxiedResponse);
    }

    private CloseableHttpResponse executeRequest(HttpRequestBase httpRequest, ContainerRequestContext request)
        throws IOException {
        CloseableHttpResponse proxiedResponse;

        // Forward all the headers
        for (Map.Entry<String, List<String>> e : request.getHeaders().entrySet()) {
            for (String headerValue : e.getValue()) {
                // TODO: Should have a safer way to treat some non-forwardable header.
                if (e.getKey().equalsIgnoreCase("Content-Length")
                    || e.getKey().equalsIgnoreCase("Transfer-Encoding")) {
                    continue;
                }
                httpRequest.setHeader(e.getKey(), headerValue);
            }
        }

        if (httpRequest instanceof HttpEntityEnclosingRequest) {
            Object requestBody = request.getProperty("requestBody");
            if (requestBody == null) {
                requestBody = IOUtils.toString(request.getEntityStream());
                request.setProperty("requestBody", requestBody);
            }
            ((HttpEntityEnclosingRequest) httpRequest).setEntity(new StringEntity((String)requestBody));
        }
        proxiedResponse = httpClient.execute(httpRequest);
        return proxiedResponse;
    }

    private Response getResponse(CloseableHttpResponse proxiedResponse) throws IOException {
        Response.ResponseBuilder builder = Response
            .status(proxiedResponse.getStatusLine().getStatusCode());

        if (proxiedResponse.getEntity() != null) {
            builder.entity(EntityUtils.toString(proxiedResponse.getEntity()));
            for (Header header : proxiedResponse.getAllHeaders()) {
                builder.header(header.getName(), header.getValue());
            }
        }
        return builder.build();
    }
}
