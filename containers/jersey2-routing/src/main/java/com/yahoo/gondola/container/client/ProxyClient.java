/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.client;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Proxy client interface.
 */
public interface ProxyClient {
    Response proxyRequest(ContainerRequestContext request, String baseUri) throws IOException;
}
