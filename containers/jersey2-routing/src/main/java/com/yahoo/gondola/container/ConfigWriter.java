/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import com.yahoo.gondola.Config;

import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Config writer supports change config value and save to file.
 */
public class ConfigWriter {

    Config config;

    Logger logger = LoggerFactory.getLogger(ConfigWriter.class);
    com.typesafe.config.Config configImpl;
    File tmpFile, configFile;

    public ConfigWriter(File configFile) {
        loadConfig(configFile);
    }

    private void loadConfig(File configFile) {
        this.configFile = configFile;
        config = new Config(configFile);
        configImpl = ConfigFactory.parseFile(configFile);
        tmpFile = getFile(true);
        tmpFile = getFile(true);
        verify();
    }

    private File getFile(boolean tmp) {
        return new File("foo");
    }

    private void verify() {
        // TODO: implement
    }

    /**
     * Save file.
     *
     * @return the file
     */
    public File save() {
        ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults()
            .setComments(true)
            .setFormatted(true)
            .setOriginComments(false)
            .setJson(false);
        bumpVersion();
        String configData = configImpl.root().render(renderOptions);
        FileWriter writer = null;
        try {
            writer = new FileWriter(tmpFile);
            writer.write(configData);
        } catch (IOException e) {
            logger.warn("failed to write file, message={}", e.getMessage());
        } finally {
            CloseableUtils.closeQuietly(writer);
        }
        return tmpFile;
    }

    private void bumpVersion() {
        int version;
        try {
            version = configImpl.getInt("version");
        } catch (ConfigException e) {
            version = 0;
        }
        version++;
        configImpl = configImpl.withValue("version", ConfigValueFactory.fromAnyRef(version));
    }

    public void setBucketMap(String shardId, String bucketMapString) {
        boolean success = false;
        List<com.typesafe.config.ConfigObject> newShards = new ArrayList<>();
        for (com.typesafe.config.Config shard : configImpl.getConfigList("gondola.shards")) {
            if (shard.getString("shardId").equals(shardId)) {
                shard = shard.withValue("bucketMap", ConfigValueFactory.fromAnyRef(bucketMapString));
                success = true;
            }
            newShards.add(shard.root());
        }
        if (!success) {
            throw new IllegalArgumentException("ShardID not found in config!" + shardId);
        }


        configImpl = configImpl.withValue("gondola.shards", ConfigValueFactory.fromIterable(newShards));
    }
}
