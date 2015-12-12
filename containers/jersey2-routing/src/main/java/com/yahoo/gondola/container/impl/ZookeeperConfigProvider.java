/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.impl;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ZookeeperUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;


/**
 * The type Zookeeper config provider.
 */
public class ZookeeperConfigProvider implements Config.ConfigProvider {

    private CuratorFramework client;
    private String serviceName;
    private NodeCache cache;
    private Path tmpFile;
    private Path confFile;
    private List<Runnable> shutdownFunctions = new ArrayList<>();
    private static Logger logger = LoggerFactory.getLogger(ZookeeperConfigProvider.class);

    /**
     * Instantiates a new Zookeeper config provider.
     */
    public ZookeeperConfigProvider(URI uri) throws Exception {
        String connectString = uri.getHost() + ":" + (uri.getPort() == -1 ? 2181 : uri.getPort());
        String serviceName = uri.getPath().split("/", 2)[1];
        client = CuratorFrameworkFactory.newClient(connectString, new RetryOneTime(1000));
        client.start();
        shutdownFunctions.add(client::close);

        initProvider(client, serviceName);
    }

    /**
     * Instantiates a new Zookeeper config provider.
     *
     * @param client      the client
     * @param serviceName the service name
     * @throws Exception the exception
     */
    public ZookeeperConfigProvider(CuratorFramework client, String serviceName) throws Exception {
        initProvider(client, serviceName);
    }

    private void initProvider(CuratorFramework client, String serviceName) throws Exception {
        this.client = client;
        this.serviceName = serviceName;
        tmpFile = confFile(true);
        confFile = confFile(false);
        String configPath = ZookeeperUtils.configPath(serviceName);

        byte[] bytes = client.getData().forPath(configPath);
        saveFile(bytes);
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
        writer.close();
        verifyConfig(tmpFile.toFile());
        Files.move(tmpFile, confFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private Path confFile(boolean tmp) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        return Paths.get(
            tmpDir + File.separator + "gondola-" + serviceName + "-" + ManagementFactory
                .getRuntimeMXBean().getName() + (tmp ? "t.conf" : ".conf"));
    }


    private void verifyConfig(File configFile) {
        Config config = new Config(configFile);
        config.stop();
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
        shutdownFunctions.forEach(Runnable::run);
        quietDelete(confFile);
        quietDelete(tmpFile);
    }

    private void quietDelete(Path file) {
        try {
            System.out.println("Delete file" + file);
            Files.deleteIfExists(file);
        } catch (IOException e) {
            // ignored
        }
    }
}
