/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import java.net.URI;

public class AdminCli {
    AdminClient adminClient;


    public static void main(String args[]){
        new AdminCli();
    }

    public AdminCli() {
        Config config = ConfigLoader.getConfigInstance(URI.create("classpath:///gondola.conf"));
        adminClient = AdminClient.getInstance(config, "adminCli");
    }
}
