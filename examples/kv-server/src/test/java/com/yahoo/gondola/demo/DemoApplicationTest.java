package com.yahoo.gondola.demo;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.RoutingFilter;

import org.testng.annotations.Test;

import java.io.File;

public class DemoApplicationTest {

    Config config = new Config(new File(DemoApplicationTest.class.getClassLoader().getResource("gondola.conf").getFile()));

    @Test
    public void testCompatibility() throws Exception {
        RoutingFilter.configCheck(config);
    }
}