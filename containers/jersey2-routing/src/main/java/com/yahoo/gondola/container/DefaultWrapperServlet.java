/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import org.eclipse.jetty.servlet.DefaultServlet;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

public class DefaultWrapperServlet extends DefaultServlet
{
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException
    {
        RequestDispatcher rd = getServletContext().getNamedDispatcher("default");

        HttpServletRequest wrapped = new HttpServletRequestWrapper(req) {
            public String getServletPath() { return ""; }
        };

        rd.forward(wrapped, resp);
    }
}
