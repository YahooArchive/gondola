/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.container.GondolaApplication;

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

    public DemoApplication(@Context ServletContext servletContext) throws Exception {
        // Initialize Routing application
        GondolaApplication.Builder.createGondolaApplication()
//            .setConfigUri(URI.create("zookeeper://localhost:2181/foo"))
            .setConfigUri(URI.create("classpath:///gondola.conf"))
            .setRoutingHelper(DemoRoutingHelper.class)
            .setService(DemoService.class)
            .setApplication(this)
            .register();

        // register resource
        register(DemoResources.class);
    }
}
