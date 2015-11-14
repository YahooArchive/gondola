/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.localserver.RequestBasicAuth;
import org.apache.http.localserver.ResponseBasicUnauthorized;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.Response;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class ApacheHttpComponentProxyClientTest extends LocalServerTestBase {

    public static final String RESPONSE_CONTENT = "foo";
    @Mock
    ContainerRequest request;

    @Mock
    ExtendedUriInfo uriInfo;

    String targetUri;

    ApacheHttpComponentProxyClient client;
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();

        HttpProcessor httpproc = HttpProcessorBuilder.create()
            .add(new ResponseDate())
            .add(new ResponseServer(LocalServerTestBase.ORIGIN))
            .add(new ResponseContent())
            .add(new ResponseConnControl())
            .add(new RequestBasicAuth())
            .add(new ResponseBasicUnauthorized()).build();
        this.serverBootstrap.setHttpProcessor(httpproc);
        this.serverBootstrap.registerHandler("*", new Handler());

        HttpHost target = start();

        targetUri = getTargetUri(target);

        MockitoAnnotations.initMocks(this);
        URI uri = URI.create(targetUri + "/test");
        when(uriInfo.getRequestUri()).thenReturn(uri);
        when(request.getUriInfo()).thenReturn(uriInfo);
        when(request.getEntityStream()).thenReturn(new ByteArrayInputStream(RESPONSE_CONTENT.getBytes()));

        client = new ApacheHttpComponentProxyClient();
    }

    private String getTargetUri(HttpHost target) {
        return target.getSchemeName() + "://" + target.getHostName() + ":" + target.getPort();
    }


    @DataProvider (name = "requestProvider")
    private Object[][] requestProvider() {
        return new Object[][] {
            {"GET"},
            {"POST"},
            {"PUT"},
            {"DELETE"},
        };
    }

    @Test (dataProvider = "requestProvider")
    public void testProxyRequest(String method) throws Exception {
        when(request.getMethod()).thenReturn(method);
        Response response = client.proxyRequest(request, targetUri + "/");
        int status = response.getStatus();
        assertEquals(RESPONSE_CONTENT, response.getEntity().toString());
        assertEquals(200, status);
    }

    class Handler implements HttpRequestHandler {

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
            throws HttpException, IOException {
            response.setStatusCode(200);
            response.setEntity(new StringEntity(RESPONSE_CONTENT));
        }
    }
}