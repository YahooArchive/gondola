/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.ConfigLoader;
import com.yahoo.gondola.container.RoutingFilter;
import com.yahoo.gondola.container.Utils;

import java.net.URI;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class DemoClient {

    Client client = ClientBuilder.newClient();

    public static void main(String args[]) throws InterruptedException {
        new DemoClient();
    }

    public DemoClient() throws InterruptedException {
        Config config = ConfigLoader.getConfigInstance(URI.create("classpath:///gondola.conf"));
        String appUri = Utils.getAppUri(config, config.getHostIds().get(0));
        String resource = "/api/entries/1";
        int value = 1;
        while (true) {
            Response
                putResponse =
                client.target(appUri).path(resource).request().put(Entity.entity(value, MediaType.TEXT_PLAIN_TYPE));
            if (putResponse.getStatus() != 204) {
                System.out.println("Error - " + putResponse);
                continue;
            }
            Response response = client.target(appUri).path(resource).request().get();
            if (response.getStatus() != 200) {
                System.out.println("Read failed" + response.getStatus());
            } else {
                String responseData  = response.readEntity(String.class);
                System.out.println(responseData);
                int currentValue  = Integer.parseInt(responseData);
                if (currentValue != value) {
                    throw new IllegalStateException("does not match! cur=" + currentValue + " val=" + value);
                }
                System.out.println("Value : " + currentValue);
                System.out.println("Current server = " + appUri);
                System.out.println("Leader server = " + response.getHeaderString(RoutingFilter.X_GONDOLA_LEADER_ADDRESS) + "\n");
            }
            value++;
            Thread.sleep(1000);
        }
    }
}
