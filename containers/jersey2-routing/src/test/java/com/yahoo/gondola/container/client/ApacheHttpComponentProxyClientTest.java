/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import org.apache.http.Header;
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

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class ApacheHttpComponentProxyClientTest extends LocalServerTestBase {
    public static final String RESPONSE_CONTENT = "foo";
    public static final String TEST_HEADER_NAME = "test_header_name";
    public static final String TEST_HEADER_VALUE = "test_header_value";
    public static final String TEST_PATH = "/test";
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
        URI uri = URI.create(targetUri + TEST_PATH);
        when(uriInfo.getRequestUri()).thenReturn(uri);
        when(request.getUriInfo()).thenReturn(uriInfo);
        when(request.getEntityStream()).thenReturn(new ByteArrayInputStream(RESPONSE_CONTENT.getBytes()));
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add(TEST_HEADER_NAME, TEST_HEADER_VALUE);
        when(request.getHeaders()).thenReturn(headers);

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
            //TODO: apache http component test server does not support PATCH method, skip the test for now.
            // {"PATCH"},
        };
    }

    @Test (dataProvider = "requestProvider")
    public void testProxyRequest(String method) throws Exception {
        when(request.getMethod()).thenReturn(method);
        Response response = client.proxyRequest(request, targetUri + "/");
        int status = response.getStatus();
        assertEquals(response.getEntity().toString(), RESPONSE_CONTENT);
        assertEquals(status, 200);
        assertEquals(response.getHeaderString(TEST_HEADER_NAME), TEST_HEADER_VALUE);
    }

    class Handler implements HttpRequestHandler {

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
            throws HttpException, IOException {
            response.setStatusCode(200);
            response.setEntity(new StringEntity(RESPONSE_CONTENT));
            Header header = request.getFirstHeader(TEST_HEADER_NAME);
            response.setHeader(header.getName(), header.getValue());
        }
    }
}