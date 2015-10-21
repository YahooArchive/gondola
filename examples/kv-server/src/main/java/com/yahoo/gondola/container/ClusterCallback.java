package com.yahoo.gondola.container;

import javax.servlet.ServletRequest;

public interface ClusterCallback {

    /**
     * The callback method to get gondola cluster ID based on request
     *
     * @param request
     * @return Gondola Cluster ID
     */
    public String getClusterId(ServletRequest request);
}
