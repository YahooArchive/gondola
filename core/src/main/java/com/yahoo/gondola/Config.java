/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola;

import com.typesafe.config.ConfigObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class supplies all of the configuration information for the entire raft system.
 * This implementation reads a config file but can be subclassed to pull configuration data from elsewhere.
 */
public class Config {
    final static Logger logger = LoggerFactory.getLogger(Config.class);

    /**
     * This interface helps getting secret in the config and it's not allowed to store in the config.
     */
    Function<String, String> secretHelper;

    /*
     * This class encapsulates all the configuration data. It's used to avoid having a lock
     * in order to retrieve the data. A new ConfigData object is created with the new data and
     * then the new ConfigData object replaces the old one by simply setting a reference.
     */
    class ConfigData {
        com.typesafe.config.Config cfg;

        /// hostId -> member[]
        Map<String, List<ConfigMember>> hostToMembers = new HashMap<>();

        // hostId -> clusterIds
        Map<String, List<String>> hostToClusters = new HashMap<>();

        // clusterId -> member[]
        Map<String, List<ConfigMember>> clusterToMembers = new HashMap<>();

        // hostId -> address
        Map<String, InetSocketAddress> addrs = new HashMap<>();

        // memberId -> member
        Map<Integer, ConfigMember> members = new HashMap<>();

        // hostId -> HostAttributes
        Map<String, Map<String, String>> hostAttributes = new HashMap<>();

        // hostId -> ClusterAttributes
        Map<String, Map<String, String>> clusterAttributes = new HashMap<>();
    }

    // Holds the latest verison of the config data. When new data is available, the new ConfigData
    // is set into this variable
    ConfigData configData;

    // Listeners for config changes
    Observable observable = new Observable() {
        @Override
        public void notifyObservers() {
            // Hack to set the change flag so that the notification will take place
            super.setChanged();
            super.notifyObservers(Config.this);
        }
    };

    // The number of ms to check the config file for changes
    int watchPeriod;

    // If non-null, the file which contains the config information
    File file;

    public static class ConfigMember {
        String clusterId;
        String hostId;
        int memberId;

        ConfigMember(String clusterId, String hostId, int memberId) {
            this.clusterId = clusterId;
            this.hostId = hostId;
            this.memberId = memberId;
        }

        public String getClusterId() {
            return clusterId;
        }

        public String getHostId() {
            return hostId;
        }

        public int getMemberId() {
            return memberId;
        }
    }

    /**
     * Reads config values from a HOCON file.
     * The observers are notified whenever the config file changes.
     */
    public Config(File file) {
        this.file = file;
        com.typesafe.config.Config cfg = com.typesafe.config.ConfigFactory.parseFile(file).resolve();
        process(cfg);
        new Watcher(file).start();
    }

    /**
     * Returns a string which describes the location of the config values. E.g. if the configs were retrieved from a file,
     * this would return the file location. Used in config-related error messages.
     *
     * @return a non-null string identifying the location of the configs.
     */
    public String getIdentifier() {
        return file == null ? "unknown" : file.toString();
    }

    /********************* simple property *******************/

    public String get(String property) {
        String secret = getSecret(property);
        return secret == null ? configData.cfg.getString(property) : secret;
    }

    public boolean getBoolean(String property) {
        String secret = getSecret(property);
        return secret == null ? configData.cfg.getBoolean(property) : Boolean.parseBoolean(secret);
    }

    public int getInt(String property) {
        String secret = getSecret(property);
        return secret == null ? configData.cfg.getInt(property) : Integer.parseInt(secret);
    }

    public long getLong(String property) {
        String secret = getSecret(property);
        return secret == null ? configData.cfg.getLong(property) : Long.parseLong(secret);
    }

    public List<String> getList(String path) {
        return configData.cfg.getList(path).stream()
            .map(configValue -> String.valueOf(configValue.unwrapped()))
            .collect(Collectors.toList());
    }

    private String getSecret(String property) {
        return secretHelper == null ? null : secretHelper.apply(property);
    }

    /********************** listener *******************/

    /**
     * The observer's update() method will be called whenever the config file is changed and reloaded.
     * The arg will be this Config object.
     */
    public void registerForUpdates(Observer obs) {
        observable.addObserver(obs);
        obs.update(observable, this);
    }

    public void registerSecretHelper(Function<String, String> helper) {
        this.secretHelper = helper;
    }

    /********************* host and cluster *******************/

    public Set<String> getHostIds() {
        return configData.hostToClusters.keySet();
    }

    /**
     * Returns the cluster ids running on a host.
     */
    public List<String> getClusterIds(String hostId) {
        List<String> clusters = configData.hostToClusters.get(hostId);
        if (clusters == null) {
            throw new IllegalArgumentException(String.format("host id '%s' not found in config", hostId));
        }
        return clusters;
    }

    /**
     * Returns all the cluster ids.
     */
    public Set<String> getClusterIds() {
        return configData.clusterToMembers.keySet();
    }

    /**
     * Returns all the members that are part of the cluster.
     * The first member returned is the primary member for that cluster.
     */
    public List<ConfigMember> getMembersInCluster(String clusterId) {
        List<ConfigMember> members = configData.clusterToMembers.get(clusterId);
        if (members == null) {
            throw new IllegalArgumentException(String.format("cluster '%s' not found in config", clusterId));
        }
        return members;
    }

    public InetSocketAddress getAddressForMember(int memberId) {
        ConfigMember cm = configData.members.get(memberId);
        if (cm == null) {
            throw new IllegalArgumentException(String.format("member '%s' not found in config", memberId));
        }
        return getAddressForHost(cm.hostId);
    }

    public InetSocketAddress getAddressForHost(String hostId) {
        InetSocketAddress addr = configData.addrs.get(hostId);
        if (addr == null) {
            throw new IllegalArgumentException(String.format("host id '%s' not found in config", hostId));
        }
        return addr;
    }

    public Map<String, String> getAttributesForHost(String hostId) {
        return configData.hostAttributes.get(hostId);
    }


    public Map<String, String> getAttributesForCluster(String clusterId) {
        return configData.clusterAttributes.get(clusterId);
    }

    public void setAddressForHostId(String hostId, InetSocketAddress socketAddress) {
        // modify the address map by copy-on-write
        Map<String, InetSocketAddress> newMap = new HashMap<>(configData.addrs);
        newMap.put(hostId, socketAddress);
        configData.addrs = newMap;
        observable.notifyObservers();
    }

    public String getSiteIdForHost(String hostId) {
        return configData.hostAttributes.get(hostId).get("siteId");
    }


    private void process(com.typesafe.config.Config cfg) {


        // Prepare the container for the new config data
        ConfigData cd = new ConfigData();
        cd.cfg = cfg;
        cd.hostToMembers = new HashMap<>();
        cd.hostToClusters = new HashMap<>();
        cd.clusterToMembers = new HashMap<>();
        cd.addrs = new HashMap<>();
        cd.members = new HashMap<>();
        cd.hostAttributes = new HashMap<>();


        // Parse the clusterid, hostid, and memberid information
        for (com.typesafe.config.Config v : cfg.getConfigList("gondola.clusters")) {
            for (com.typesafe.config.Config h : v.getConfigList("hosts")) {
                ConfigMember cm = new ConfigMember(v.getString("clusterId"), h.getString("hostId"), h.getInt("memberId"));
                // update host to members
                List<ConfigMember> cmembers = cd.hostToMembers.get(cm.clusterId);
                if (cmembers == null) {
                    cmembers = new ArrayList<>();
                    cd.hostToMembers.put(cm.clusterId, cmembers);
                }
                cmembers.add(cm);

                // Update clusterToMembers
                cmembers = cd.clusterToMembers.get(cm.clusterId);
                if (cmembers == null) {
                    cmembers = new ArrayList<>();
                    cd.clusterToMembers.put(cm.clusterId, cmembers);
                }
                cmembers.add(cm);

                // Update hostToClusters
                List<String> names = cd.hostToClusters.get(cm.hostId);
                if (names == null) {
                    names = new ArrayList<>();
                    cd.hostToClusters.put(cm.hostId, names);
                }
                names.add(cm.clusterId);
                cd.members.put(cm.memberId, cm);
            }
        }
        loadAttributes(cfg, cd.hostAttributes, "gondola.hosts", "hostId");
        loadAttributes(cfg, cd.clusterAttributes, "gondola.clusters", "clusterId");

        // Get all the addresses of the hosts
        for (com.typesafe.config.Config c : cfg.getConfigList("gondola.hosts")) {
            cd.addrs.put(c.getString("hostId"), new InetSocketAddress(c.getString("host"), c.getInt("port")));
        }

        // Enable the new config data
        this.configData = cd;
        watchPeriod = getInt("gondola.config_reload_period");
    }

    private void loadAttributes(com.typesafe.config.Config cfg, Map<String, Map<String, String>> attributesMap, String configSet, String keyName) {
        // Get host attribute map
        for (ConfigObject h : cfg.getObjectList(configSet)) {
            String hostId = String.valueOf(h.get(keyName).unwrapped());
            Map<String, String> hostAttribute = new HashMap<>();
            attributesMap.put(hostId, hostAttribute);
            for (String key : h.keySet()) {
                hostAttribute.put(key, String.valueOf(h.get(key).unwrapped()));
            }
        }
    }

    public class Watcher extends Thread {
        final File file;

        public Watcher(File file) {
            this.file = file;
            setName("ConfigWatcher");
            setDaemon(true);
        }

        @Override
        public void run() {
            long lastModified = file.lastModified();
            while (true) {
                try {
                    sleep(watchPeriod);

                    if (file.lastModified() != lastModified) {
                        // Parse and update observers
                        process(com.typesafe.config.ConfigFactory.parseFile(file));
                        observable.notifyObservers();

                        logger.info("Reloaded config file {}", file);
                        lastModified = file.lastModified();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
