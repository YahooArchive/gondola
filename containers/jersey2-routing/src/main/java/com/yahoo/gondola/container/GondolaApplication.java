/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.AdminServlet;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.google.common.base.Preconditions;
import com.purej.vminspect.http.servlet.VmInspectionServlet;
import com.yahoo.gondola.Config;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.annotation.WebListener;

/**
 * Gondola application.
 */
public class GondolaApplication {

    static private ResourceConfig application;
    static private RoutingFilter routingFilter;
    static private ShardManagerServer shardManagerServer;

    private GondolaApplication() {
    }

    public static ResourceConfig getApplication() {
        return application;
    }

    public static RoutingFilter getRoutingFilter() {
        return routingFilter;
    }

    public static ShardManagerServer getShardManagerServer() {
        return shardManagerServer;
    }

    /**
     * Builder class.
     */
    public static class Builder {

        Gondola gondola;
        Class<? extends RoutingHelper> routingHelperClass;
        ProxyClientProvider proxyClientProvider;
        ShardManagerProvider shardManagerProvider;
        Class<? extends RoutingService> serviceClass;
        URI configUri;
        ResourceConfig application;

        public static Builder createGondolaApplication() {
            return new Builder();
        }

        Builder() {
            proxyClientProvider = new ProxyClientProvider();
        }

        public Builder setProxyClientProvider(ProxyClientProvider proxyClientProvider) {
            this.proxyClientProvider = proxyClientProvider;
            return this;
        }

        public Builder setShardManagerProvider(ShardManagerProvider shardManagerProvider) {
            this.shardManagerProvider = shardManagerProvider;
            return this;
        }

        public Builder setRoutingHelper(Class<? extends RoutingHelper> routingHelperClass) {
            this.routingHelperClass = routingHelperClass;
            return this;
        }

        public Builder setService(Class<? extends RoutingService> serviceClass) {
            this.serviceClass = serviceClass;
            return this;
        }

        public Builder setConfigUri(URI configUri) {
            this.configUri = configUri;
            return this;
        }

        public Builder setApplication(ResourceConfig application) {
            this.application = application;
            return this;
        }

        public void register()
            throws ServletException, GondolaException, NoSuchMethodException, IllegalAccessException,
                   InvocationTargetException, InstantiationException {
            Preconditions.checkState(routingHelperClass != null, "RoutingHelper instance must be set");
            Preconditions.checkState(serviceClass != null, "Service class must be set");
            Preconditions.checkState(configUri != null, "Config URI must be set");
            Preconditions.checkState(application != null, "Application instance must be set");
            gondola = createGondolaInstance();
            Map<String, RoutingService> services = new HashMap<>();

            for (Shard shard : gondola.getShardsOnHost()) {
                RoutingService service =
                    serviceClass.getConstructor(Gondola.class, String.class).newInstance(gondola, shard.getShardId());
                services.put(shard.getShardId(), service);
            }

            ChangeLogProcessor changeLogProcessor = new ChangeLogProcessor(gondola, services);
            RoutingHelper routingHelper = routingHelperClass.newInstance();
            RoutingFilter routingFilter =
                new RoutingFilter(gondola, routingHelper, proxyClientProvider, services, changeLogProcessor);
            gondola.start();
            routingFilter.start();

            initShardManagerServer(routingFilter);

            // register dependency injection.
            application.register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(routingFilter).to(RoutingFilter.class);
                    bindFactory(RoutingServiceFactory.class).to(serviceClass);
                }
            });
            application.register(routingFilter);
            application.register(GondolaAdminResource.class);
            application.register(AdminResource.class);
            application.register(JacksonFeature.class);

            application.property(ServerProperties.MONITORING_STATISTICS_MBEANS_ENABLED, true);
            GondolaApplication.routingFilter = routingFilter;
            GondolaApplication.application = application;
        }

        private Gondola createGondolaInstance() throws GondolaException {
            Config config = ConfigLoader.getConfigInstance(configUri);
            String hostId = null;
            for (String h : config.getHostIds()) {
                if (Utils.isMyAddress(config.getAddressForHost(hostId).getAddress())) {
                    hostId = h;
                }
            }
            if (hostId == null) {
                throw new IllegalStateException("Cannot find IP address on the host.");
            }
            return new Gondola(config, hostId);
        }

        private void initShardManagerServer(RoutingFilter routingFilter) {
            if (shardManagerProvider == null) {
                shardManagerProvider = new ShardManagerProvider();
            }

            ShardManagerServer
                shardManagerServer =
                shardManagerProvider.getShardManagerServer(routingFilter);
            if (GondolaApplication.shardManagerServer == null && shardManagerServer != null) {
                GondolaApplication.shardManagerServer = shardManagerServer;
                routingFilter.registerShutdownFunction(shardManagerServer::stop);
            }
        }
    }

    /**
     * WebApp context listener.
     */
    @WebListener
    public static class ContextListener implements ServletContextListener {

        @Override
        public void contextInitialized(ServletContextEvent sce) {
            ServletContext context = sce.getServletContext();
            ServletRegistration.Dynamic
                servlet =
                context.addServlet("VmInspectionServlet", VmInspectionServlet.class);
            servlet.addMapping("/gondola/inspect");
            servlet.setLoadOnStartup(1);

            ServletRegistration.Dynamic
                servlet2 =
                context.addServlet("MetricsServlet", AdminServlet.class);
            servlet2.addMapping("/gondola/metrics/*");
            servlet2.setLoadOnStartup(0);

            ServletRegistration.Dynamic servlet3 =
                context.addServlet("static", DefaultWrapperServlet.class);
            servlet3.setInitParameter("cacheControl", "max-age=0,no-cache");
            servlet3.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
            servlet3.setInitParameter("welcomeServlets", "false");
            servlet3.addMapping("/static/*");
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
        }
    }


    /**
     * Metrics registry.
     */
    @WebListener
    public static class MyMetricsServletContextListener extends MetricsServlet.ContextListener {

        public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

        @Override
        protected MetricRegistry getMetricRegistry() {
            return METRIC_REGISTRY;
        }

    }

    /**
     * Healthcheck registry.
     */
    @WebListener
    public static class MyHealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

        public static final HealthCheckRegistry HEALTH_CHECK_REGISTRY = new HealthCheckRegistry();

        @Override
        protected HealthCheckRegistry getHealthCheckRegistry() {
            return HEALTH_CHECK_REGISTRY;
        }

    }
}
