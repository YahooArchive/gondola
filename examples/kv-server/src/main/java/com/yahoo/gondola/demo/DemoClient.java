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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Key value service command line client.
 */
public class DemoClient {

    public static final int N_THREADS = 16;
    Client client = ClientBuilder.newClient();
    Logger logger = LoggerFactory.getLogger(DemoClient.class);
    ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);

    public static void main(String[] args) throws InterruptedException {
        new DemoClient();
    }

    public DemoClient() throws InterruptedException {
        Config config = ConfigLoader.getConfigInstance(URI.create("classpath:///gondola.conf"));
        String appUri = Utils.getAppUri(config, config.getHostIds().get(0));
        for (int i = 0; i < N_THREADS; i++) {
            executorService.execute(getRunnable(appUri, i));
        }
    }

    private Runnable getRunnable(String appUri, int id) {
        return () -> {
            try {
                Thread.sleep(1000);
                String resource = "/api/entries/" + id;
                EntryResource data;
                while (true) {
                    while ((data = getData(appUri, resource)) == null) {
                        Thread.sleep(1000);
                    }
                    int value = data.value;
                    while (true) {
                        while (!putData(appUri, resource, value + 1)) {
                            logger.warn("write data failed, retry...");
                            Thread.sleep(1000);
                        }

                        while ((data = getData(appUri, resource)) == null) {
                            logger.warn("read data failed, retry...");
                            Thread.sleep(1000);
                        }

                        //            logger.info("Resource: {}, BucketId: {}, shardID: {}, value:{}",
                        //                        resource, data.bucketId, data.shardId, data.value);
                        //
                        //            logger.info("Request path = {} -> {}", appUri, data.gondolaLeaderAddress);

                        if (data.value != value + 1) {
                            logger.error(
                                "Data inconsistency! got value={}, expect={}, shardId={}, bucketId={}, leader={}",
                                data.value, value + 1, data.shardId, data.bucketId, data.gondolaLeaderAddress);
                            throw new IllegalStateException();
                        }

                        value++;
                        Thread.sleep(100);
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("client interrupted");
                return;
            } catch (Exception e) {
                logger.warn("error.. retries message={}, class={}", e.getMessage(), e.getClass());
            }
        };
    }

    private boolean putData(String appUri, String resource, int value) {
        Response
            putResponse =
            client.target(appUri).path(resource).request().put(Entity.entity(value, MediaType.TEXT_PLAIN_TYPE));
        try {
            if (putResponse.getStatus() != 204) {
                logger.error("Error - {}", putResponse);
                return false;
            }
            return true;
        } finally {
            putResponse.close();
        }
    }

    private EntryResource getData(String appUri, String resource) {
        Response response = client.target(appUri).path(resource).request().get();
        try {
            if (response.getStatus() != 200 && response.getHeaderString(RoutingFilter.X_GONDOLA_BUCKET_ID) == null) {
                return null;
            }

            int data;
            if (response.getStatus() == 404) {
                data = -1;
            } else if (response.getStatus() == 200) {
                data = Integer.parseInt(response.readEntity(String.class));
            } else {
                return null;
            }
            String bucketId = response.getHeaderString(RoutingFilter.X_GONDOLA_BUCKET_ID);
            String shardId = response.getHeaderString(RoutingFilter.X_GONDOLA_SHARD_ID);
            String leaderAddress = response.getHeaderString(RoutingFilter.X_GONDOLA_LEADER_ADDRESS);

            return new EntryResource(data, bucketId, shardId, leaderAddress);
        } finally {
            response.close();
        }
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
