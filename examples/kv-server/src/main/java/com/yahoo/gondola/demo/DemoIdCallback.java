package com.yahoo.gondola.demo;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.ClusterIdCallback;

import javax.servlet.ServletRequest;

public class DemoIdCallback implements ClusterIdCallback {
    public DemoIdCallback() {
        System.out.println("Demo callback initialized");
    }

    @Override
    public String getClusterId(Gondola gondola, ServletRequest request) {
        return "cluster1";
    }
}
