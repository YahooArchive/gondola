package com.yahoo.gondola.demo;

import com.yahoo.gondola.container.ClusterCallback;

import javax.servlet.ServletRequest;

public class DemoCallback implements ClusterCallback {

    @Override
    public String getClusterId(ServletRequest request) {
        return "cluster1";
    }
}
