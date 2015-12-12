/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Admin servlet. provides the admin features & page to maintain gondola application.
 */
public class AdminServlet extends HttpServlet {
    private ResourceConfig application;
    private RoutingFilter routingFilter;

    public AdminServlet() {
        this.application = GondolaApplication.getApplication();
        this.routingFilter = GondolaApplication.getRoutingFilter();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    }
}
