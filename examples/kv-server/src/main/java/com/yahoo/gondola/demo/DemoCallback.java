package com.yahoo.gondola.demo;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.ClusterCallback;

import javax.servlet.ServletRequest;

public class DemoCallback implements ClusterCallback {
    public DemoCallback() {
        System.out.println("Demo callback initialized");
    }

    @Override
    public String getClusterId(Gondola gondola, ServletRequest request) {
        return "cluster1";
    }
}
