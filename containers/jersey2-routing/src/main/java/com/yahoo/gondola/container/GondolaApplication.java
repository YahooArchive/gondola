/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.google.common.base.Preconditions;
import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.GondolaException;
import com.yahoo.gondola.Shard;
import com.yahoo.gondola.container.spi.RoutingHelper;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;

public class GondolaApplication {

    private GondolaApplication() {
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

            if (shardManagerProvider == null) {
                shardManagerProvider = new ShardManagerProvider(gondola.getConfig());
            }
            ChangeLogProcessor changeLogProcessor = new ChangeLogProcessor(gondola, services);
            RoutingHelper routingHelper = routingHelperClass.newInstance();
            RoutingFilter routingFilter =
                new RoutingFilter(gondola, routingHelper, proxyClientProvider, services, changeLogProcessor);
            initShardManagerServer(routingFilter);
            gondola.start();
            routingFilter.start();

            // register dependency injection.
            application.register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(routingFilter).to(RoutingFilter.class);
                    bindFactory(RoutingServiceFactory.class).to(serviceClass);
                }
            });
            application.register(routingFilter);
        }

        private Gondola createGondolaInstance() throws GondolaException {
            String hostId = System.getenv("hostId") != null ? System.getenv("hostId") : "host1";
            return new Gondola(ConfigLoader.getConfigInstance(configUri), hostId);
        }

        private void initShardManagerServer(RoutingFilter routingFilter) {
            ShardManagerServer shardManagerServer = shardManagerProvider.getShardManagerServer();
            if (shardManagerServer != null) {
                ShardManager shardManager =
                    new ShardManager(gondola, routingFilter, gondola.getConfig(),
                                     shardManagerProvider.getShardManagerClient());
                shardManagerServer.setShardManager(shardManager);
                routingFilter.registerShutdownFunction(shardManagerServer::stop);
            }
        }
    }

}
