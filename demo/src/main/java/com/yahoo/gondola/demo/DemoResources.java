/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.demo;

import javax.inject.Inject;
import javax.ws.rs.*;

@Produces("application/json")
@Consumes("application/json")
@Path("/entries/{entryId}")
public class DemoResources {
    @Inject DemoService service;

    @GET
    public Node getEntry(@PathParam("nodeId") String entryId) {
        return service.getEntry(entryId);
    }

    @PUT
    public void putEntry(Entry entry, @PathParam("entryId") String entryId) {
        service.putEntry(entryId, node);
    }
}
