/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

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
        verify();
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
        ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults().setJson(false);
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
        // TODO: bump config version
    }

    public void setBucketMap(String shardId, String bucketMapString) {
        configImpl = configImpl
            .withValue("gondola.shards." + shardId, ConfigValueFactory.fromAnyRef(bucketMapString));
    }
}
