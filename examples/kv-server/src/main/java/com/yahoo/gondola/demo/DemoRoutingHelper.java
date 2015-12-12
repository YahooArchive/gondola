/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import com.yahoo.gondola.container.spi.RoutingHelper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * The callback that inspect the request and return a shard ID for RoutingFilter.
 */
public class DemoRoutingHelper implements RoutingHelper {

    @Override
    public int getBucketHash(ContainerRequestContext request) {
        return hashUri(request.getUriInfo().getPath());
    }

    /**
     * The URI will be relative to servlet. e.g: entries/key1?foo=123
     */
    private int hashUri(String path) {
        Pattern pattern = Pattern.compile("entries/([^/?]+)");
        Matcher m = pattern.matcher(path);
        return m.find() ? m.group(0).hashCode() : 0;
    }
}
