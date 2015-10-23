package com.yahoo.gondola.demo;

import com.yahoo.gondola.Gondola;
import com.yahoo.gondola.container.ClusterIdCallback;

import javax.ws.rs.container.ContainerRequestContext;

public class DemoIdCallback implements ClusterIdCallback {
    Gondola gondola;
    public DemoIdCallback(Gondola gondola) {
        this.gondola = gondola;
    }

    @Override
    public String getClusterId(ContainerRequestContext request) {
        return "cluster1";
    }
}
