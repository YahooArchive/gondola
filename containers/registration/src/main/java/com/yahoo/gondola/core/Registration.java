/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.core;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * <p>The Registration client interface, which talks to a centralized registration service.</p>
 *
 * <b>Usage:</b>
 * <blockquote><pre>
 *     CuratorFramework client = ...;
 *     client.start();
 *     ObjectMapper mapper = new ObjectMapper();
 *     Registration registration = new ZookeeperRegistration(client, mapper);
 *     // Register for getting updates, and make sure observer is registered before start() to prevent missing update event
 *     registration.addObserver(registry1 -> {
 *        ..
 *     });
 *     registration.start();
 *     // Register for memberId
 *     InetAddress address = getGondolaAddress();
 *     int memberId = registration.register("cluster1", address, "site1", "http://api1.yahoo.com:4080");
 *     int memberId2 = registration.register("cluster2", address, "site1", "http://api1.yahoo.com:4080");
 *     int memberId3 = registration.register("cluster3", address, "site1", "http://api1.yahoo.com:4080");
 *     // Wait for joined cluster until all other nodes joined
 *     registration.await(-1);
 *
 *     File gondolaConfig = registration.getConfig();
 *
 *     Config config = new Config(gondolaConfig);
 *     for (String hostId : registration.getHostIds()) {
 *       Gondola gondola = new Gondola(config, hostId);
 *       gondola.start();
 *       ...
 *     }
 * </pre></blockquote>
 */
public interface Registration {

    /**
     * Register for a hostId in specific site
     *
     * @param clusterId     Which cluster to register
     * @param serverAddress Gondola ip:port that used for inter gondola communication
     * @param siteId        the siteId of the host
     * @param serviceURI    The application service URI
     * @return registered memberId for gondola
     * @throws RegistrationException the registration exception
     */
    int register(String clusterId, InetAddress serverAddress, String siteId, URI serviceURI) throws
                                                                                             RegistrationException;

    /**
     * Add listener for host list changes
     */
    void addObserver(RegistrationObserver registrationObserver);

    /**
     * Get registry map
     *
     * @return A map of registries, key is the memberId
     */
    Map<Integer, Registry> getRegistries();

    /**
     * Wait until other nodes joined the cluster we just registered
     *
     * @param timeoutMs if timeout < 0, means infinite
     */
    void await(int timeoutMs);

    /**
     * Get gondola config from Registry service
     *
     * @return the file of the configuration
     */
    File getConfig();

    /**
     * get registered hostIds
     */
    List<String> getHostIds();

    /**
     * start client service
     */
    void start();

    /**
     * stop client service
     */
    void stop();

    class RegistrationException extends RuntimeException {

        public RegistrationException(Exception e) {
            super(e);
        }

        public RegistrationException(String s) {
            super(s);
        }
    }

    class Registry {

        public String hostId;
        public InetAddress serverAddress;
        public String siteId;
        public String clusterId;
        public int memberId;
    }

    interface RegistrationObserver {

        void registrationUpdate(Registry registry);
    }
}
