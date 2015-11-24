/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.container.RoutingFilter;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.function.Consumer;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

/**
 * Jersey2 JAX-RS application class, it initialize the essential class before serving.
 *
 * 1. Initialize Gondola instance 2. Initialize business logic object - DemoService 3. Initialize routing filter
 * callback and register RoutingFilter as Jersey filter 4. Register the resources
 */
public class DemoApplication extends ResourceConfig {

    static Logger logger = LoggerFactory.getLogger(DemoApplication.class);
    Gondola gondola;

    static String hostId = System.getenv("hostId") != null ? System.getenv("hostId") : "host1";

    public DemoApplication(@Context ServletContext servletContext) throws Exception {
        gondola = initializeGondola();

        // Dependency injection to DemoResource
        DemoService demoService = new DemoService(gondola);
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(demoService).to(DemoService.class);
            }
        });

        // Register routing filter
        register(RoutingFilter.Builder.createRoutingFilter()
                     .setRoutingHelper(new DemoRoutingHelper(gondola, demoService))
                     .setGondola(gondola)
                     .build());

        // register resource
        register(DemoResources.class);
    }

    private Gondola initializeGondola() throws Exception {
        // Find the config file
        URL gondolaConfURI = DemoApplication.class.getClassLoader().getResource("gondola.conf");
        if (gondolaConfURI == null) {
            throw new FileNotFoundException(String.format("Gondola configuration '%s' not found", "gondola.conf"));
        }

        // Create the gondola instance
        File gondolaConf = new File(gondolaConfURI.getFile());
        Config config = new Config(gondolaConf);
        Gondola gondola = new Gondola(config, hostId);

        // Register for role updates and start gondola
        logger.info("Current role: FOLLOWER");
        Consumer<RoleChangeEvent> listener = crevt -> {
            switch (crevt.newRole) {
                case CANDIDATE:
                    logger.info("[{}] Current role: CANDIDATE", gondola.getHostId());
                    break;
                case LEADER:
                    logger.info("[{}] Current role: LEADER", gondola.getHostId());
                    break;
                case FOLLOWER:
                    logger.info("[{}] Current role: FOLLOWER", gondola.getHostId());
                    break;
            }
        };
        gondola.registerForRoleChanges(listener);
        gondola.start();
        return gondola;
    }
}
