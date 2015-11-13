/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Proxy client implementation using Apache Http Client
 */
public class ApacheHttpComponentProxyClient implements ProxyClient {
    /**
     * The Httpclient.
     */
    CloseableHttpClient httpClient;

    public ApacheHttpComponentProxyClient() {
        httpClient = HttpClients.createDefault();
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
                HttpGet httpGet = new HttpGet(baseUri + requestURI);
                proxiedResponse = httpClient.execute(httpGet);
                break;
            case "PUT":
                HttpPut httpPut = new HttpPut(baseUri + requestURI);
                httpPut.setHeader(HTTP.CONTENT_TYPE, request.getHeaderString("Content-Type"));
                httpPut.setEntity(new InputStreamEntity(request.getEntityStream()));
                proxiedResponse = httpClient.execute(httpPut);
                break;
            case "POST":
                HttpPost httpPost = new HttpPost(baseUri + requestURI);
                httpPost.setHeader(HTTP.CONTENT_TYPE, request.getHeaderString("Content-Type"));
                httpPost.setEntity(new InputStreamEntity(request.getEntityStream()));
                proxiedResponse = httpClient.execute(httpPost);
                break;
            case "DELETE":
                HttpDelete httpDelete = new HttpDelete(baseUri + requestURI);
                httpDelete.setHeader(HTTP.CONTENT_TYPE, request.getHeaderString("Content-Type"));
                proxiedResponse = httpClient.execute(httpDelete);
                break;
            default:
                throw new IllegalStateException("Method not supported: " + method);
        }

        return getResponse(proxiedResponse);
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
