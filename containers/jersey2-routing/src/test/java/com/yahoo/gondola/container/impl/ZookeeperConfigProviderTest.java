/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container.impl;

import com.yahoo.gondola.container.client.ZookeeperUtils;
import com.yahoo.gondola.container.utils.ZookeeperServer;

import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;

import static org.testng.Assert.assertTrue;

public class ZookeeperConfigProviderTest {

    ZookeeperServer server;
    ZookeeperConfigProvider configProvider;
    URL file = ZookeeperConfigProviderTest.class.getClassLoader().getResource("gondola.conf");

    @BeforeMethod
    public void setUp() throws Exception {
        if (server == null) {
            server = new ZookeeperServer();
            server.getClient().create().creatingParentContainersIfNeeded().forPath(ZookeeperUtils.configPath("fooService"), IOUtils.toByteArray(file));
        }
        configProvider = new ZookeeperConfigProvider(server.getClient(), "fooService");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        configProvider.stop();
    }

    @Test
    public void testGetConfigFile() throws Exception {
        File configFile = configProvider.getConfigFile();
        assertTrue(Arrays.equals(IOUtils.toByteArray(configFile.toURI()), IOUtils.toByteArray(file)));
    }

    // TODO: fix race condition
//    @Test
//    public void testStop() throws Exception {
//        configProvider.stop();
//    }
//
//    @Test
//    public void testSaveConfigFile() throws Exception {
//        configProvider.saveConfigFile(new File(file.getFile()));
//    }
}