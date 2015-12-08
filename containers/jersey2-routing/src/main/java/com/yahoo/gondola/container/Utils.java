/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yahoo.gondola.Config;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Utility class.
 */
public class Utils {

    static final String APP_PORT = "appPort";
    static final String APP_SCHEME = "appScheme";
    static ObjectMapper objectMapper;

    /**
     * Polling with timeout utility function, accept a boolean supplier that throws Exception. It'll retry until the
     * retryCount reached or there's timeout.
     *
     * @param timeoutMs -1 means no limitation, 0 means no wait.
     * @return true if success, false if timeout reached.
     */
    public static boolean pollingWithTimeout(CheckedBooleanSupplier supplier, long waitTimeMs, long timeoutMs)
        throws InterruptedException, ExecutionException {
        return pollingWithTimeout(supplier, waitTimeMs, timeoutMs, null, null);
    }


    public static boolean pollingWithTimeout(CheckedBooleanSupplier supplier, long waitTimeMs, long timeoutMs,
                                             Lock lock, Condition condition)
        throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        // TODO: default timeout MS should be provided by upper layer.
        if (timeoutMs < 0) {
            waitTimeMs = 1000;
        }
        try {
            while (timeoutMs <= 0 || System.currentTimeMillis() - start < timeoutMs) {
                lock(lock);
                try {
                    if (supplier.getAsBoolean()) {
                        return true;
                    }
                    long remain = timeoutMs - (System.currentTimeMillis() - start);
                    if (timeoutMs == 0) {
                        break;
                    }
                    long timeout = timeoutMs == -1 || waitTimeMs < remain || remain <= 0 ? waitTimeMs : remain;
                    wait(lock, condition, timeout);
                } finally {
                    unlock(lock);
                }
            }
            return false;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private static void wait(Lock lock, Condition condition, long timeout) throws InterruptedException {
        if (lock != null) {
            condition.await(timeout, TimeUnit.MILLISECONDS);
        } else {
            Thread.sleep(timeout);
        }
    }

    private static void unlock(Lock lock) {
        if (lock != null) {
            lock.unlock();
        }
    }

    private static void lock(Lock lock) {
        if (lock != null) {
            lock.lock();
        }
    }

    /**
     * A functional interface that return boolean value and throw Exception if any error.
     */
    @FunctionalInterface
    public interface CheckedBooleanSupplier {

        boolean getAsBoolean() throws Exception;
    }

    public static String getAppUri(Config config, String hostId) {
        InetSocketAddress address = config.getAddressForHost(hostId);
        Map<String, String> attrs = config.getAttributesForHost(hostId);
        String
            appUri =
            String.format("%s://%s:%s", attrs.get(APP_SCHEME), address.getHostName(), attrs.get(APP_PORT));
        if (!attrs.containsKey(APP_PORT) || !attrs.containsKey(APP_SCHEME)) {
            throw new IllegalStateException(
                String
                    .format("gondola.hosts[%s] is missing either the %s or %s config values", hostId, APP_PORT,
                            APP_SCHEME));
        }
        return appUri;
    }

    public static ObjectMapper getObjectMapperInstance() {
        if (objectMapper == null) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);
            mapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
            mapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
            objectMapper = mapper;
        }
        return objectMapper;
    }

    public static RegistryConfig getRegistryConfig(Config config) {
        RegistryConfig conf = new RegistryConfig();
        conf.attributes = new HashMap<>();
        try {
            String impl = config.get("registry.impl");

            switch (impl) {
                case "registry.zookeeper":
                    conf.type = Type.ZOOKEEPER;
                    conf.attributes.put("connectString", config.get("registry.zookeeper.connect_string"));
                    conf.attributes.put("serviceName", config.get("registry.zookeeper.service_name"));
                    break;
                case "registry.http":
                    conf.type = Type.HTTP;
                    break;
                default:
                    throw new IllegalArgumentException("Impl=" + impl + " does not supported");
            }
        } catch (Exception e) {
            conf.type = Type.NONE;
        }
        return conf;
    }

    enum Type {NONE, HTTP, ZOOKEEPER}

    /**
     * Registry config.
     */
    public static class RegistryConfig {
        Type type;
        Map<String, String> attributes;
    }

}
