package com.yahoo.gondola.container;

import com.yahoo.gondola.Gondola;

import javax.servlet.ServletRequest;

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
public interface ClusterCallback {
    /**
     * The callback method to get gondola cluster ID based on request
     *
     * @param gondola
     * @param request
     * @return Gondola Cluster ID
     */
    public String getClusterId(Gondola gondola, ServletRequest request);
}
