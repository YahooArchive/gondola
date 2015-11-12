/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Endpoint and resource definition for the kv-server service.
 */
@Path("/entries/{key}")
public class DemoResources {
    Logger logger = LoggerFactory.getLogger(DemoResources.class);

    @Inject DemoService service;

    @GET
    public String getEntry(@PathParam("key") String key) {
        try {
            return service.getValue(key);
        } catch (DemoService.NotLeaderException e) {
            throw new ServiceUnavailableException();
        } catch (DemoService.NotFoundException e) {
            throw new NotFoundException();
        }
    }

    @PUT
    public void putEntry(String value, @PathParam("key") String key) {
        try {
            service.putValue(key, value);
        } catch (DemoService.NotLeaderException e) {
            throw new ServiceUnavailableException();
        } catch (Throwable t) {
            logger.error("Server Error", t);
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, t);
        }
    }
}
