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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class supplies all of the configuration information for the entire raft system. This implementation reads a
 * config file but can be subclassed to pull configuration data from elsewhere.
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

        // hostId -> shardIds
        Map<String, List<String>> hostToShards = new HashMap<>();

        // shardId -> member[]
        Map<String, List<ConfigMember>> shardToMembers = new HashMap<>();

        // hostId -> address
        Map<String, InetSocketAddress> addrs = new HashMap<>();

        // memberId -> member
        Map<Integer, ConfigMember> members = new HashMap<>();

        // hostId -> HostAttributes
        Map<String, Map<String, String>> hostAttributes = new HashMap<>();

        // hostId -> shardAttributes
        Map<String, Map<String, String>> shardAttributes = new HashMap<>();
    }

    // Holds the latest verison of the config data. When new data is available, the new ConfigData
    // is set into this variable
    ConfigData configData;

    // Listeners.
    List<Consumer<Config>> listeners = new ArrayList<>();

    // The number of ms to check the config file for changes
    int watchPeriod;

    // If non-null, the file which contains the config information
    File file;

    /**
     * The type Config member.
     */
    public static class ConfigMember {

        String shardId;
        String hostId;
        int memberId;

        ConfigMember(String shardId, String hostId, int memberId) {
            this.shardId = shardId;
            this.hostId = hostId;
            this.memberId = memberId;
        }

        public String getShardId() {
            return shardId;
        }

        public String getHostId() {
            return hostId;
        }

        public int getMemberId() {
            return memberId;
        }
    }

    /**
     * Reads config values from a HOCON file. The observers are notified whenever the config file changes.
     */
    public Config(File file) {
        this.file = file;
        process(getMergedConf(file));
        new Watcher(file).start();
    }

    /*
     * Returns the config file merged with the settings in the default.conf file.
     */
    com.typesafe.config.Config getMergedConf(File file) {
        com.typesafe.config.Config cfg = com.typesafe.config.ConfigFactory.parseFile(file).resolve();

        InputStream resourceStream = Gondola.class.getClassLoader().getResourceAsStream("default.conf");
        if (resourceStream == null) {
            throw new IllegalStateException("default.conf not found");
        }
        com.typesafe.config.Config
            defaultCfg =
            com.typesafe.config.ConfigFactory.parseReader(new InputStreamReader(resourceStream)).resolve();
        return cfg.withFallback(defaultCfg);
    }


    /**
     * Returns a string which describes the location of the config values. E.g. if the configs were retrieved from a
     * file, this would return the file location. Used in config-related error messages.
     *
     * @return a non-null string identifying the location of the configs.
     */
    public String getIdentifier() {
        return file == null ? "unknown" : file.toString();
    }

    /*********************
     * simple property
     *******************/

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
     * The observer's update() method will be called whenever the config file is changed and reloaded. The arg will be
     * this Config object.
     */
    public void registerForUpdates(Consumer<Config> listener) {
        listener.accept(this);
        listeners.add(listener);
    }

    public void registerSecretHelper(Function<String, String> helper) {
        this.secretHelper = helper;
    }

    /*********************
     * host and shard
     *******************/

    public Set<String> getHostIds() {
        return configData.hostToShards.keySet();
    }

    /**
     * Returns the shard ids running on a host.
     */
    public List<String> getShardIds(String hostId) {
        List<String> shards = configData.hostToShards.get(hostId);
        if (shards == null) {
            throw new IllegalArgumentException(String.format("host id '%s' not found in config", hostId));
        }
        return shards;
    }

    /**
     * Returns all the shard ids.
     */
    public Set<String> getShardIds() {
        return configData.shardToMembers.keySet();
    }

    /**
     * Returns all the members that are part of the shard. The first member returned is the primary member for that
     * shard.
     */
    public List<ConfigMember> getMembersInShard(String shardId) {
        List<ConfigMember> members = configData.shardToMembers.get(shardId);
        if (members == null) {
            throw new IllegalArgumentException(String.format("shard '%s' not found in config", shardId));
        }
        return members;
    }

    /**
     * Returns all the members that are part of the host.
     */
    public List<ConfigMember> getMembersInHost(String hostId) {
        List<ConfigMember> members = configData.hostToMembers.get(hostId);
        if (members == null) {
            throw new IllegalArgumentException(String.format("hostId '%s' not found in config", hostId));
        }
        return members;
    }

    /**
     * Returns all the members that are part of the host.
     */
    public List<ConfigMember> getMembers() {
        return new ArrayList<>(configData.members.values());
    }

    public InetSocketAddress getAddressForMember(int memberId) {
        ConfigMember cm = configData.members.get(memberId);
        if (cm == null) {
            throw new IllegalArgumentException(String.format("member '%s' not found in config", memberId));
        }
        return getAddressForHost(cm.hostId);
    }

    public ConfigMember getMember(int memberId) {
        ConfigMember cm = configData.members.get(memberId);
        if (cm == null) {
            throw new IllegalArgumentException(String.format("member '%s' not found in config", memberId));
        }
        return cm;
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

    public Map<String, String> getAttributesForShard(String shardId) {
        return configData.shardAttributes.get(shardId);
    }

    public void setAddressForHostId(String hostId, InetSocketAddress socketAddress) {
        // modify the address map by copy-on-write
        Map<String, InetSocketAddress> newMap = new HashMap<>(configData.addrs);
        newMap.put(hostId, socketAddress);
        configData.addrs = newMap;
        notifyListener(this);
    }

    public String getSiteIdForHost(String hostId) {
        return configData.hostAttributes.get(hostId).get("siteId");
    }

    private void process(com.typesafe.config.Config cfg) {
        // Prepare the container for the new config data
        ConfigData cd = new ConfigData();
        cd.cfg = cfg;
        cd.hostToMembers = new HashMap<>();
        cd.hostToShards = new HashMap<>();
        cd.shardToMembers = new HashMap<>();
        cd.addrs = new HashMap<>();
        cd.members = new HashMap<>();
        cd.hostAttributes = new HashMap<>();

        // Parse the shardid, hostid, and memberid information
        for (com.typesafe.config.Config v : cfg.getConfigList("gondola.shards")) {
            for (com.typesafe.config.Config h : v.getConfigList("hosts")) {
                ConfigMember cm = new ConfigMember(v.getString("shardId"), h.getString("hostId"), h.getInt("memberId"));
                // update host to members
                List<ConfigMember> cmembers = cd.hostToMembers.get(cm.hostId);
                if (cmembers == null) {
                    cmembers = new ArrayList<>();
                    cd.hostToMembers.put(cm.hostId, cmembers);
                }
                cmembers.add(cm);

                // Update shardToMembers
                cmembers = cd.shardToMembers.get(cm.shardId);
                if (cmembers == null) {
                    cmembers = new ArrayList<>();
                    cd.shardToMembers.put(cm.shardId, cmembers);
                }
                cmembers.add(cm);

                // Update hostToShards
                List<String> names = cd.hostToShards.get(cm.hostId);
                if (names == null) {
                    names = new ArrayList<>();
                    cd.hostToShards.put(cm.hostId, names);
                }
                names.add(cm.shardId);
                cd.members.put(cm.memberId, cm);
            }
        }
        loadAttributes(cfg, cd.hostAttributes, "gondola.hosts", "hostId");
        loadAttributes(cfg, cd.shardAttributes, "gondola.shards", "shardId");

        // Get all the addresses of the hosts
        for (com.typesafe.config.Config c : cfg.getConfigList("gondola.hosts")) {
            cd.addrs.put(c.getString("hostId"), new InetSocketAddress(c.getString("hostname"), c.getInt("port")));
        }

        // Enable the new config data
        this.configData = cd;
        watchPeriod = getInt("gondola.config_reload_period");
    }

    private void loadAttributes(com.typesafe.config.Config cfg, Map<String, Map<String, String>> attributesMap,
                                String configSet, String keyName) {
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

    private void notifyListener(Config config) {
        listeners.forEach(l -> l.accept(config));
    }

    /**
     * The Config Watcher thread.
     */
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
                        process(getMergedConf(file));
                        notifyListener(Config.this);

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
