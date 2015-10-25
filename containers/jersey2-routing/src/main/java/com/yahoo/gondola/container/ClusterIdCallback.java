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
 * <p> When initialization
 * <ul>
 *   <li>RoutingFilter initialize the callback.</li>
 *   <li>Application initialize the Gondola and set to RoutingFilter's static variable.</li>
 * </ul>
 * </p>
 *
 * <p> When processing the request
 *     <ul>
 *        <li>RoutingFilter pass gondola instance and servlet request to the callback.</li>
 *     </ul>
 * </p>
 *
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
