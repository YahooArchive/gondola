/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * <p>The Registry client interface, which talks to a registry service.</p>
 *
 * <b>Registration Process</b>
 * <blockquote><pre>
 *     Config config = new Config(new File("gondola.conf"));
 *     <b>RegistryClient registry = RegistryClients.createZookeeperClient(config);</b>
 *     <b>registry.addListener(registry -> {});</b>
 *     InetAddress address = getGondolaAddress();
 *     URL serviceUri = getServiceUri();
 *     String siteId = getSiteId();
 *     <b>String hostId = registry.register(address, siteId, serviceUri)</b>;
 *     <b>registry.await(-1);</b>
 *
 *     Gondola gondola = new Gondola(config, hostId);
 *     gondola.start();
 *
 * </pre></blockquote>
 *
 * <b>Get entries</b>
 * <blockquote>
 * <pre>
 *     List&lt;Entry&gt; entries = registry.getEntries();
 * </pre>
 * </blockquote>
 */
public interface RegistryClient {

    /**
     * Register for a hostId in specific site
     *
     * @param siteId         The siteId of the host
     * @param serverAddress  Gondola ip:port that used for inter gondola communication
     * @param serviceURI     The application service URI
     * @return registered hostId for gondola
     * @throws RegistryException the registration exception
     */
    String register(String siteId, InetSocketAddress serverAddress, URI serviceURI) throws RegistryException;

    /**
     * Add listener for host list changes
     */
    void addListener(Consumer<Entry> observer);

    /**
     * Get registry map
     *
     * @return A map of entries, key is the memberId
     */
    Map<String, Entry> getEntries();

    /**
     * Wait until other nodes joined the cluster we just registered
     *
     * @param timeoutMs if timeout < 0, means infinite
     */
    boolean await(int timeoutMs) throws RegistryException;

    class RegistryException extends RuntimeException {

        public RegistryException(Exception e) {
            super(e);
        }

        public RegistryException(String s) {
            super(s);
        }
    }

    class Entry {

        public String hostId;
        public String siteId;
        public List<Integer> memberIds;
        public List<String> clusterIds;
        public InetSocketAddress serverAddress;
    }
}
