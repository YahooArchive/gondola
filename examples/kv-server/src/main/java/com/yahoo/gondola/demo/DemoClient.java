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

import java.io.IOException;
import java.net.URI;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class DemoClient {

    Client client = ClientBuilder.newClient();

    public static void main(String args[]) throws InterruptedException, IOException {
        new DemoClient();
    }

    public DemoClient() throws InterruptedException, IOException {
        Config config = ConfigLoader.getConfigInstance(URI.create("classpath:///gondola.conf"));
        String appUri = Utils.getAppUri(config, config.getHostIds().get(0));
        String resource = "/api/entries/1";
        EntryResource data;
        while ((data = getData(appUri, resource)) == null) {
            Thread.sleep(1000);
        }
        int value = data.value;
        while (true) {
            while (!putData(appUri, resource, value + 1)) {
                Thread.sleep(1000);
            }

            while ((data = getData(appUri, resource)) == null) {
                Thread.sleep(1000);
            }

            System.out.println(
                "Resource: " + resource
                + ", BucketId: " + data.bucketId
                + ", shardID: " + data.shardId
                + ", value=" + data.value);

            System.out.println("Request path = " + appUri + " -> " + data.gondolaLeaderAddress + "\n");

            if (data.value != value + 1) {
                throw new IllegalStateException(
                    "Data inconsistency! got value=" + data.value + ", expect=" + value + 1);
            }

            value++;
            Thread.sleep(1000);
        }
    }

    private boolean putData(String appUri, String resource, int value) {
        Response
            putResponse =
            client.target(appUri).path(resource).request().put(Entity.entity(value, MediaType.TEXT_PLAIN_TYPE));
        if (putResponse.getStatus() != 204) {
            System.out.println("Error - " + putResponse);
            return false;
        }
        return true;
    }

    private EntryResource getData(String appUri, String resource) {
        Response response = client.target(appUri).path(resource).request().get();
        if (response.getStatus() != 200 && response.getHeaderString(RoutingFilter.X_GONDOLA_BUCKET_ID) == null) {
            return null;
        }

        int data;
        if (response.getStatus() == 404) {
            data = -1;
        } else {
            data = Integer.parseInt(response.readEntity(String.class));
        }
        String bucketId = response.getHeaderString(RoutingFilter.X_GONDOLA_BUCKET_ID);
        String shardId = response.getHeaderString(RoutingFilter.X_GONDOLA_SHARD_ID);
        String leaderAddress = response.getHeaderString(RoutingFilter.X_GONDOLA_LEADER_ADDRESS);

        return new EntryResource(data, bucketId, shardId, leaderAddress);
    }

    class EntryResource {

        int value;
        String bucketId;
        String shardId;
        String gondolaLeaderAddress;

        public EntryResource(int value, String bucketId, String shardId, String gondolaLeaderAddress) {
            this.value = value;
            this.bucketId = bucketId;
            this.shardId = shardId;
            this.gondolaLeaderAddress = gondolaLeaderAddress;
        }
    }
}
