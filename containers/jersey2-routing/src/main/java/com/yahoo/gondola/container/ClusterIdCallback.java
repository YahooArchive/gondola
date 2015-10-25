/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * How callbacks work:
 *
 * Initialization
 * 1. RoutingFilter initialize the callback
 * 2. Application initialize the Gondola and set to RoutingFilter's static varialbe
 *
 * Request phase
 * 3. While processing the request, RoutingFilter pass gondola instance and servlet request to the callback.
 */
public interface ClusterIdCallback {
    /**
     * The callback method to get gondola cluster ID based on request
     *
     * @param request
     * @return Gondola Cluster ID
     */
    String getClusterId(ContainerRequestContext request);
}
