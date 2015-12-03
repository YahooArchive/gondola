/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * The factory implementation for dependency injection.
 */
public class RoutingServiceFactory implements Factory<RoutingService> {

    ContainerRequestContext request;

    @Inject
    RoutingFilter routingFilter;

    @Inject
    public RoutingServiceFactory(ContainerRequestContext request) {
        this.request = request;
    }

    @Override
    public RoutingService provide() {
        return routingFilter.getService(request);
    }

    @Override
    public void dispose(RoutingService instance) {
        // doing nothing
    }
}
