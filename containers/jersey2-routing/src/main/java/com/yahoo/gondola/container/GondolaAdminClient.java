/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

public class GondolaAdminClient {

    public static final String API_SET_LEADER = "/api/gondola/v1/local/setLeader";
    public static final String API_GONDOLA_STATUS = "/api/gondola/v1/local/gondolaStatus";
    public static final String API_INSPECT_REQUEST_URI = "/api/gondola/v1/local/inspectRequestUri";
    Config config;
    Client client = ClientBuilder.newClient();
    Logger logger = LoggerFactory.getLogger(GondolaAdminClient.class);

    public GondolaAdminClient(Config config) {
        this.config = config;
    }

    public Map setLeader(String hostId, String shardId) {
        String appUri = Utils.getAppUri(config, hostId);
        WebTarget target = client.target(appUri)
            .path(API_SET_LEADER)
            .queryParam("shardId", shardId);
        return target.request().post(null, Map.class);
    }


    public Map getHostStatus(String hostId) {
        String appUri = Utils.getAppUri(config, hostId);
        WebTarget target = client.target(appUri).path(API_GONDOLA_STATUS);
        try {
            return target.request(MediaType.APPLICATION_JSON_TYPE).get(Map.class);
        } catch (Exception e) {
            logger.warn("Cannot get remote host hostId={}, message={}", hostId, e.getMessage());
            return null;
        }
    }

    public Map<String, Object> getServiceStatus() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (String hostId : config.getHostIds()) {
            map.put(hostId, getHostStatus(hostId));
        }
        return map;
    }

    public Map inspectRequestUri(String uri, String hostId) {
        String appUri = Utils.getAppUri(config, hostId);
        return client.target(appUri).path(API_INSPECT_REQUEST_URI)
            .queryParam("requestUri", uri).request().get(Map.class);
    }
}
