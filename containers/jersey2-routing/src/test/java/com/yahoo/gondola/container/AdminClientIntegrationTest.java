/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;

public class AdminClientIntegrationTest {
    URL configFileURL = AdminClientIntegrationTest.class.getClassLoader().getResource("gondola.conf");
    Config config = new Config(new File(configFileURL.getFile()));

    @Test
    public void testSetServiceName() throws Exception {

    }

    @Test
    public void testGetServiceName() throws Exception {

    }

    @Test
    public void testGetConfig() throws Exception {

    }

    @Test
    public void testSetConfig() throws Exception {

    }

    @Test
    public void testSplitShard() throws Exception {

    }

    @Test
    public void testMergeShard() throws Exception {

    }

    @Test
    public void testEnable() throws Exception {

    }

    @Test
    public void testDisable() throws Exception {

    }

    @Test
    public void testGetStats() throws Exception {

    }

    @Test
    public void testEnableTracing() throws Exception {

    }

    @Test
    public void testDisableTracing() throws Exception {

    }
}