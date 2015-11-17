/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import org.apache.http.HttpHost;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.localserver.RequestBasicAuth;
import org.apache.http.localserver.ResponseBasicUnauthorized;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;

public class LocalTestServer extends LocalServerTestBase {

    HttpHost host;
    public LocalTestServer(HttpRequestHandler handler) {
        try {
            setUp();
            HttpProcessor httpproc = HttpProcessorBuilder.create()
                .add(new ResponseDate())
                .add(new ResponseServer(LocalServerTestBase.ORIGIN))
                .add(new ResponseContent())
                .add(new ResponseConnControl())
                .add(new RequestBasicAuth())
                .add(new ResponseBasicUnauthorized()).build();
            this.serverBootstrap.setHttpProcessor(httpproc);
            this.serverBootstrap.registerHandler("*", handler);
            host = start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public HttpHost getHost() {
        return host;
    }
}
