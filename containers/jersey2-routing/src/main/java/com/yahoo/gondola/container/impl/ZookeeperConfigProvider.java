/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.impl;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ZookeeperUtils;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;


/**
 * The type Zookeeper config provider.
 */
public class ZookeeperConfigProvider implements Config.ConfigProvider {

    private CuratorFramework client;
    private String serviceName;
    private NodeCache cache;
    private Path tmpFile;
    private Path confFile;

    Logger logger = LoggerFactory.getLogger(ZookeeperConfigProvider.class);

    /**
     * Instantiates a new Zookeeper config provider.
     *
     * @param client      the client
     * @param serviceName the service name
     * @throws Exception the exception
     */
    public ZookeeperConfigProvider(CuratorFramework client, String serviceName) throws Exception {
        this.client = client;
        this.serviceName = serviceName;
        tmpFile = confFile(true);
        confFile = confFile(false);
        String configPath = ZookeeperUtils.configPath(serviceName);
        saveFile(client.getData().forPath(configPath));
        cache = new NodeCache(client, configPath);
        cache.getListenable().addListener(() -> {
            try {
                if (cache.getCurrentData() != null) {
                    byte[] data = cache.getCurrentData().getData();
                    if (data.length > 0) {
                        saveFile(data);
                    }
                }
            } catch (Exception e) {
                logger.warn("Error while processing config file change event. message={}", e.getMessage());
            }
        });
        cache.start();
    }

    private void saveFile(byte[] bytes) throws IOException {
        FileWriter writer = new FileWriter(tmpFile.toFile());
        writer.write(new String(bytes));
        writer.flush();
        writer.close();
        verifyConfig(tmpFile.toFile());
        Files.move(tmpFile, confFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private Path confFile(boolean tmp) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        return Paths.get(
            tmpDir + File.separator + "gondola-" + serviceName + "." + (tmp ? Thread.currentThread().getId() : "conf"));
    }

    @Override
    public void saveConfigFile(File configFile) {
        verifyConfig(configFile);
        try {
            // TODO: transaction here.
            client.setData().forPath(ZookeeperUtils.configPath(serviceName), IOUtils.toByteArray(configFile.toURI()));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot write config file to remote server.");
        }
    }

    private void verifyConfig(File configFile) {
        new Config(configFile);
    }

    /**
     * Gets config file.
     *
     * @return the config file
     */
    @Override
    public File getConfigFile() {
        return confFile.toFile();
    }

    /**
     * Stop config fetching daemon.
     */
    @Override
    public void stop() {
        CloseableUtils.closeQuietly(cache);
    }
}
