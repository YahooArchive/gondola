/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.container.RoutingFilter;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

/**
 * Jersey2 JAX-RS application class, it initialize the essential class before serving.
 *
 * 1. Initialize Gondola instance 2. Initialize business logic object - DemoService 3. Initialize routing filter
 * callback and register RoutingFilter as Jersey filter 4. Register the resources
 */
public class DemoApplication extends ResourceConfig {
    DemoService demoService;

    public DemoApplication(@Context ServletContext servletContext) throws Exception {

        // Initialize Routing application
        RoutingFilter routingFilter = RoutingFilter.Builder.createRoutingFilter()
//            .setConfigUri(URI.create("zookeeper://localhost:2181/foo"))
            .setConfigUri(URI.create("classpath:///gondola.conf"))
            .setService(DemoService.class)
            .build();

        // Register routing application
        register(routingFilter);

        // Dependency injection to DemoResource
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind((DemoService) routingFilter.getService()).to(DemoService.class);
            }
        });

        // register resource
        register(DemoResources.class);
    }
}
