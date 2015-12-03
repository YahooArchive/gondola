/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.impl.ZookeeperConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * The type Config loader.
 */
public class ConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);


    private ConfigLoader() {
        // prevent Config Loader to instantiate.
    }
    /**
     * Load config file.
     *
     * @param configUri the config uri
     * @return the file
     */
    public static Config getConfigInstance(URI configUri) {
        Config config;
        File configFile;
        switch (configUri.getScheme()) {
            case "classpath":
                URL resource = Thread.currentThread().getContextClassLoader().getResource(configUri.getPath());
                if (resource == null) {
                    throw new IllegalArgumentException("File not found in " + configUri.toString());
                }
                configFile = new File(resource.getFile());
                config = new Config(configFile);
                break;
            case "file":
                configFile = new File(configUri.getPath());
                config = new Config(configFile);
                break;
            case "zookeeper":
                try {
                    ZookeeperConfigProvider zookeeperConfigProvider = new ZookeeperConfigProvider(configUri);
                    configFile = zookeeperConfigProvider.getConfigFile();
                    config = new Config(zookeeperConfigProvider);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                throw new IllegalArgumentException("Scheme not supported - " + configUri.getScheme());
        }
        logger.info("Load config from {} - local file={}", configUri.getScheme(), configFile);
        return config;
    }
}
