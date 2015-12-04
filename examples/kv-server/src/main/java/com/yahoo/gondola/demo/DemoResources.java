/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.Response;

/**
 * Endpoint and resource definition for the kv-server service.
 */
@Path("/entries/{key}")
public class DemoResources {

    Logger logger = LoggerFactory.getLogger(DemoResources.class);

    /* Dependency injection for service */
    @Inject
    DemoService service;

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
