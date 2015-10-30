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
 * The Registration client interface, which talks to a centralized registration service.
 *
 * <b>Usage:</b>
 * <blockquote><pre>
 *     Registration registration = new ZookeeperRegistration(...);
 *     // Register for getting updates
 *     registration.addObserver(registry1 -> {
 *        ..
 *     });
 *     // Register for memberId
 *     InetAddress address = getGondolaAddress();
 *     int memberId = registration.register("cluster1", address, "site1", "http://api1.yahoo.com:4080");
 *     int memberId2 = registration.register("cluster2", address, "site1", "http://api1.yahoo.com:4080");
 *     int memberId3 = registration.register("cluster3", address, "site1", "http://api1.yahoo.com:4080");
 *     // Wait for joined clusters have all other nodes joined
 *     registration.await("cluster1", -1);
 *     registration.await("cluster2", -1);
 *     registration.await("cluster3", -1);
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
     * Wait until other nodes join the cluster
     *
     * @param clusterId
     * @param timeoutMs if timeout < 0, means infinite
     */
    void await(String clusterId, int timeoutMs);

    /**
     * Get gondola config from Registry service
     *
     * @return the file of the configuration
     */
    File getConfig();

    /**
     * get registered hostIds
     *
     * @return
     */
    List<String> getHostIds();

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
