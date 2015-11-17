/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import com.yahoo.gondola.container.LocalTestServer;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class ApacheHttpComponentProxyClientTest {
    public static final String RESPONSE_CONTENT = "foo";
    public static final String TEST_HEADER_NAME = "test_header_name";
    public static final String TEST_HEADER_VALUE = "test_header_value";
    public static final String TEST_PATH = "/test";
    @Mock
    ContainerRequest request;

    @Mock
    ExtendedUriInfo uriInfo;

    String targetUri;

    LocalTestServer server = new LocalTestServer((request1, response, context) -> {
        response.setStatusCode(200);
        response.setEntity(new StringEntity(RESPONSE_CONTENT));
        Header header = request1.getFirstHeader(TEST_HEADER_NAME);
        response.setHeader(header.getName(), header.getValue());
    });

    ApacheHttpComponentProxyClient client;
    @BeforeMethod
    public void setUp() throws Exception {
        targetUri = getTargetUri(server.getHost());

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
}