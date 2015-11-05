/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
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
 *     <b>registry.waitForClusterComplete(-1);</b>
 *
 *     Gondola gondola = new Gondola(config, hostId);
 *     gondola.start();
 *
 * </pre></blockquote>
 *
 * <b>Get entries</b> <blockquote>
 * <pre>
 *     List&lt;Entry&gt; entries = registry.getEntries();
 * </pre>
 * </blockquote>
 */
public interface RegistryClient {

    /**
     * Register for a hostId in specific site.
     *
     * @param siteId        The siteId of the host.
     * @param gondolaAddress Gondola ip:port that used for inter gondola communication.
     * @param serviceUri    This URI represents the URL prefix to use when forwarding requests to this node.
     * @return registered hostId for gondola.
     * @throws IOException
     */
    String register(String siteId, InetSocketAddress gondolaAddress, URI serviceUri) throws IOException;

    /**
     * Add registryChangeHandler for host list changes.
     */
    void addListener(Consumer<Entry> observer);

    /**
     * Get registry map.
     *
     * @return A map of entries, key is the hostId.
     */
    Map<String, Entry> getEntries();

    /**
     * Causes the current thread to wait until other nodes joined the cluster, or the specified waiting time elapses.
     *
     * @param timeoutMs if timeout < 0, means infinite.
     * @return success if all required nodes join the cluster
     * @throws IOException
     */
    boolean waitForClusterComplete(int timeoutMs) throws IOException;

    /**
     * The host entry in this registry.
     */
    class Entry {
        public String hostId;
        public InetSocketAddress gondolaAddress;
    }
}
