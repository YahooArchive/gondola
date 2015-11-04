/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.demo;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.ProxyClientProvider;
import com.yahoo.gondola.container.RoutingFilter;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.core.Context;

/**
 * Jersey2 JAX-RS application class,
 * it initialize the essential class before serving.
 *
 * 1. Initialize Gondola instance
 * 2. Initialize business logic object - DemoService
 * 3. Initialize routing filter callback and register RoutingFilter as Jersey filter
 * 4. Register the resources
 */
public class DemoApplication extends ResourceConfig {
    Gondola gondola;
    static DemoApplication instance;
    static Logger logger = LoggerFactory.getLogger(DemoApplication.class);

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

        // register routing filter
        RoutingFilter routingFilter = new RoutingFilter(gondola,
                                                        new DemoRoutingHelper(gondola, demoService),
                                                        new ProxyClientProvider());
        register(routingFilter);

        // register resources in the package
        packages(true, "com.yahoo.gondola.demo");
        instance = this;
    }

    public static DemoApplication getInstance() {
        return instance;
    }

    private Gondola initializeGondola() throws Exception {
        URL gondolaConfURI = DemoApplication.class.getClassLoader().getResource("gondola.conf");
        if (gondolaConfURI == null) {
            throw new FileNotFoundException("Gondola configuration not found");
        }
        File gondolaConf = new File(gondolaConfURI.getFile());
        Config config = new Config(gondolaConf);
        String hostId = System.getenv("hostId") != null ? System.getenv("hostId") : "host1";
        Gondola gondola = new Gondola(config, hostId);
        gondola.start();
        return gondola;
    }

    /**
     * Servlet 3.0 context listener, used to manage the lifecycle of the app.
     */
    @WebListener
    public static class ContextListener implements ServletContextListener {
        @Override
        public void contextInitialized(ServletContextEvent sce) {
            // doing nothing
            logger.info("Demo application initialized");
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            logger.info("Demo application destroyed");
            DemoApplication.getInstance().gondola.stop();
        }
    }
}
