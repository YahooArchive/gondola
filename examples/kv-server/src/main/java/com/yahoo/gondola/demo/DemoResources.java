/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.demo;

import javax.inject.Inject;
import javax.ws.rs.*;

@Path("/entries/{entryId}")
public class DemoResources {
    @Inject DemoService service;

    @GET
    public String getEntry(@PathParam("entryId") String entryId) {
        try {
            return service.getValue(entryId);
        } catch (DemoService.RecordNotFoundException e) {
            throw new NotFoundException();
        }
    }

    @PUT
    public void putEntry(String value, @PathParam("entryId") String entryId) {
        service.putValue(entryId, value);
    }
}
